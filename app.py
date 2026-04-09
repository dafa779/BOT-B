import os
import re
import time
import asyncio
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv
import uvicorn

from db import (
    init_db,
    get_setting,
    set_setting,
    delete_setting,
    get_admin,
    add_admin,
    remove_admin,
    get_all_admins,
    save_group,
    get_groups,
    add_operator,
    remove_operator,
    clear_operators,
    get_operators,
    get_global_operators,
    is_operator,
    save_member,
    get_members,
    add_transaction,
    get_transaction,
    get_last_transaction,
    undo_transaction,
    clear_transactions,
    get_transactions,
)

# ================= ENV =================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "8080"))
SUPER_ADMIN_ID = int(os.getenv("SUPER_ADMIN_ID", "0") or 0)

# ================= BOT =================
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML", link_preview_is_disabled=True)
)
dp = Dispatcher(storage=MemoryStorage())

# ================= DB =================
init_db()


def init_super_admin():
    if SUPER_ADMIN_ID:
        try:
            add_admin(SUPER_ADMIN_ID, "super")
        except Exception as e:
            print("init_super_admin error:", e)


# ================= STATES =================
class BroadcastFSM(StatesGroup):
    waiting_content = State()
    waiting_confirm = State()


# ================= HELPERS =================
def is_cmd(message: types.Message, *cmds):
    if not message.text:
        return False
    head = message.text.strip().split()[0].lower()
    head = head.split("@")[0]
    return head in [c.lower() for c in cmds]


def is_group_message(message: types.Message):
    return message.chat.type in ("group", "supergroup")


def is_private(message: types.Message):
    return message.chat.type == "private"


def fmt_num(x):
    if x is None:
        return "0"
    try:
        x = float(x)
        if abs(x - int(x)) < 1e-9:
            return str(int(x))
        return f"{x:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return str(x)


def get_chat_setting(chat_id, key, default=None):
    v = get_setting(chat_id, key, None)
    if v is None:
        v = get_setting(-1, key, default)
    return v if v is not None else default


def set_chat_setting(chat_id, key, value):
    set_setting(chat_id, key, value)


def ensure_group(m: types.Message):
    if is_group_message(m):
        save_group(m.chat.id, m.chat.title or "Unnamed group")
        if m.from_user:
            save_member(
                m.chat.id,
                m.from_user.id,
                m.from_user.username or "",
                m.from_user.full_name
            )


def get_rate(chat_id):
    return float(get_chat_setting(chat_id, "rate", "190"))


def get_fee(chat_id):
    return float(get_chat_setting(chat_id, "fee", "7"))


def get_enabled(chat_id):
    return str(get_chat_setting(chat_id, "enabled", "0")) == "1"


def is_admin_or_operator(chat_id, user: types.User):
    if get_admin(user.id) in ("super", "admin"):
        return True
    return is_operator(chat_id, user.id, user.username or "")


def menu_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="开始记账"),
                KeyboardButton(text="申请试用"),
                KeyboardButton(text="使用说明"),
            ],
            [
                KeyboardButton(text="群发广播"),
                KeyboardButton(text="分组功能"),
                KeyboardButton(text="自助续费"),
            ],
            [
                KeyboardButton(text="实时U价"),
                KeyboardButton(text="管理客服"),
                KeyboardButton(text="地址查询"),
            ],
        ],
        resize_keyboard=True
    )


def report_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📘 完整账单", callback_data="report:full")]
    ])


def parse_user_ref(msg: types.Message):
    """
    优先：reply_to_message 的用户
    否则：@username
    """
    if msg.reply_to_message and msg.reply_to_message.from_user:
        u = msg.reply_to_message.from_user
        return u.id, (u.username or ""), u.full_name

    parts = (msg.text or "").split()
    for p in parts[1:]:
        if p.startswith("@"):
            return None, p[1:].strip(), p
    return None, None, None


def parse_rate_fee_suffix(text):
    # 支持：/7.8  *12%
    rate = None
    fee = None

    m = re.search(r"/\s*(-?\d+(?:\.\d+)?)", text)
    if m:
        rate = float(m.group(1))

    m = re.search(r"\*\s*(-?\d+(?:\.\d+)?)%", text)
    if m:
        fee = float(m.group(1))

    return rate, fee


def split_target_prefix(text):
    """
    把 '张三+1000' / '张三 下发1000' 拆成 target + body
    """
    t = text.strip()
    markers = ["下发", "P+", "P-", "+", "-"]
    for mk in markers:
        pos = t.find(mk)
        if pos > 0:
            target = t[:pos].strip()
            body = t[pos:].strip()
            if target:
                return target, body
    return None, t


def parse_amount_expr(expr, chat_id, default_direct_unit=False):
    """
    expr examples:
      +10000
      -10000/7.8
      +10000*12%
      +10000/7.8*12%
      +7777u
      1000R (handled via suffix R)
    """
    rate_default = get_rate(chat_id)
    fee_default = get_fee(chat_id)

    expr = expr.replace(" ", "")
    m = re.fullmatch(
        r"(?P<sign>[+\-]?)"
        r"(?P<amt>\d+(?:\.\d+)?)"
        r"(?P<suffix>[uUrR]?)"
        r"(?:/(?P<rate>\d+(?:\.\d+)?))?"
        r"(?:\*(?P<fee>-?\d+(?:\.\d+)?)%)?",
        expr
    )
    if not m:
        return None

    sign = -1 if m.group("sign") == "-" else 1
    amt = float(m.group("amt"))
    suffix = (m.group("suffix") or "").lower()
    rate_override = float(m.group("rate")) if m.group("rate") else None
    fee_override = float(m.group("fee")) if m.group("fee") else None

    rate_used = rate_override if rate_override is not None else rate_default
    fee_used = fee_override if fee_override is not None else fee_default

    # direct U
    if suffix == "u" or (default_direct_unit and suffix == "" and rate_override is None and fee_override is None):
        unit_amount = sign * amt
        raw_amount = None
        display = f"{fmt_num(unit_amount)}U"
        return {
            "raw_amount": raw_amount,
            "unit_amount": unit_amount,
            "rate_used": rate_used,
            "fee_used": fee_used,
            "display": display
        }

    # convert from raw amount
    if rate_used == 0:
        return None

    net_ratio = 1 - (fee_used / 100.0)
    unit_amount = sign * amt / rate_used * net_ratio
    raw_amount = sign * amt
    display = f"{fmt_num(amt)} / {fmt_num(rate_used)} * ({net_ratio:.2f}) = {fmt_num(unit_amount)}U"
    return {
        "raw_amount": raw_amount,
        "unit_amount": unit_amount,
        "rate_used": rate_used,
        "fee_used": fee_used,
        "display": display
    }


def extract_note(text, body):
    # body may be "+1000 备注"
    rest = body[len(text):].strip()
    return rest if rest else ""


def format_tx_line(tx, show_target=True):
    tx_id, chat_id, user_id, username, display_name, target_name, kind, raw_amount, unit_amount, rate_used, fee_used, note, original_text, created_at, undone = tx
    tm = datetime.fromtimestamp(created_at).strftime("%H:%M:%S")

    prefix = f"{tm} "
    if kind == "reserve":
        return f"{prefix}{fmt_num(unit_amount)}U {target_name or note or ''}".strip()

    if raw_amount is not None:
        line = f"{prefix}{fmt_num(raw_amount)} / {fmt_num(rate_used)} * ({1 - fee_used/100:.2f})={fmt_num(unit_amount)}U"
    else:
        line = f"{prefix}{fmt_num(unit_amount)}U"

    extra = []
    if show_target and target_name:
        extra.append(target_name)
    if note:
        extra.append(note)

    if extra:
        line += " " + " ".join(extra)
    return line.strip()


def summarize_transactions(txs):
    income = [t for t in txs if t[6] == "income"]
    payout = [t for t in txs if t[6] == "payout"]
    reserve = [t for t in txs if t[6] == "reserve"]

    total_income_unit = sum((t[8] or 0) for t in income)
    total_payout_unit = sum((t[8] or 0) for t in payout)
    total_reserve_unit = sum((t[8] or 0) for t in reserve)

    due = total_income_unit
    paid = total_payout_unit
    pending = due - paid + total_reserve_unit

    total_raw_income = sum((abs(t[7]) or 0) for t in income if t[7] is not None)

    return {
        "income_count": len(income),
        "payout_count": len(payout),
        "reserve_count": len(reserve),
        "total_income_unit": total_income_unit,
        "total_payout_unit": total_payout_unit,
        "total_reserve_unit": total_reserve_unit,
        "due": due,
        "paid": paid,
        "pending": pending,
        "total_raw_income": total_raw_income,
    }


def day_range(ts=None):
    if ts is None:
        ts = int(time.time())
    dt = datetime.fromtimestamp(ts)
    start = datetime(dt.year, dt.month, dt.day)
    end = start + timedelta(days=1) - timedelta(seconds=1)
    return int(start.timestamp()), int(end.timestamp())


def month_range(offset_months=0):
    now = datetime.now()
    year = now.year
    month = now.month - offset_months
    while month <= 0:
        month += 12
        year -= 1
    start = datetime(year, month, 1)
    if month == 12:
        nxt = datetime(year + 1, 1, 1)
    else:
        nxt = datetime(year, month + 1, 1)
    end = nxt - timedelta(seconds=1)
    return int(start.timestamp()), int(end.timestamp())


def report_text(chat_id, start_ts, end_ts, title="账单"):
    txs = get_transactions(chat_id, start_ts=start_ts, end_ts=end_ts)
    stats = summarize_transactions(txs)

    income_txs = [t for t in txs if t[6] == "income"]
    payout_txs = [t for t in txs if t[6] == "payout"]
    reserve_txs = [t for t in txs if t[6] == "reserve"]

    lines = [f"📒 <b>{title}</b>"]

    lines.append(f"\n<b>今日入款（{len(income_txs)}笔）</b>")
    if income_txs:
        for tx in income_txs:
            lines.append(format_tx_line(tx))
    else:
        lines.append("暂无入款")

    lines.append(f"\n<b>今日下发（{len(payout_txs)}笔）</b>")
    if payout_txs:
        for tx in payout_txs:
            lines.append(format_tx_line(tx))
    else:
        lines.append("暂无下发")

    if reserve_txs:
        lines.append(f"\n<b>账单寄存（{len(reserve_txs)}笔）</b>")
        for tx in reserve_txs:
            lines.append(format_tx_line(tx))

    lines.append(f"\n<b>分组统计（{len(get_groups())}组）</b>")
    # 这里只做简单 group 统计：按 target_name 聚合
    group_map = {}
    for tx in income_txs:
        key = tx[5] or "未命名"
        group_map.setdefault(key, 0.0)
        group_map[key] += float(tx[8] or 0)

    if group_map:
        for k, v in group_map.items():
            lines.append(f"{k} 入:{fmt_num(v)}")
    else:
        lines.append("暂无分组数据")

    lines.append("")
    lines.append(f"总入款：{fmt_num(stats['total_raw_income'])} ({fmt_num(stats['total_income_unit'])}U)")
    lines.append(f"汇率：{fmt_num(get_rate(chat_id))}")
    lines.append(f"交易费率：{fmt_num(get_fee(chat_id))}%")
    lines.append("")
    lines.append(f"应下发：{fmt_num(stats['due'])}  |  {fmt_num(stats['due'])}U")
    lines.append(f"已下发：{fmt_num(stats['paid'])}  |  {fmt_num(stats['paid'])}U")
    lines.append(f"未下发：{fmt_num(stats['pending'])}  |  {fmt_num(stats['pending'])}U")

    return "\n".join(lines)


def help_text():
    return (
        "📚 <b>记账机器人操作说明</b>\n\n"
        "<b>基础功能</b>\n"
        "• 开始记账：开始\n"
        "• 停止记账：关闭记账\n"
        "• 打开发言：上课\n"
        "• 停止发言：下课\n\n"
        "<b>配置</b>\n"
        "• 设置汇率190\n"
        "• 设置费率7\n"
        "• 设置手续费20\n"
        "• 代付费率-5\n"
        "• 代付汇率8\n"
        "• 设置火币汇率\n"
        "• 设置欧易汇率\n"
        "• 设置实时汇率\n\n"
        "<b>操作员</b>\n"
        "• @xxxx 添加操作员\n"
        "• @xxxx 删除操作员\n"
        "• 显示操作员\n"
        "• 回复某人：添加操作员 / 删除操作员\n\n"
        "<b>记账</b>\n"
        "• +10000\n"
        "• +10000/7.8\n"
        "• -10000\n"
        "• -10000/7.8\n"
        "• +7777u\n"
        "• -7777u\n"
        "• 下发5000\n"
        "• 下发-2000\n"
        "• 下发1000R\n"
        "• +1000 空格备注\n"
        "• P+2000 / P-1000\n\n"
        "<b>查看</b>\n"
        "• 账单 / /我\n"
        "• 我的账单（仅操作员）\n"
        "• 总账单\n"
        "• 上个月总账单\n"
        "• 撤销\n"
        "• 重置 / 清零 / 删除账单 / 结束账单\n\n"
        "<b>广播</b>\n"
        "• 群发广播\n\n"
        "<b>提示</b>\n"
        "• 回复账单消息可撤销最近一笔\n"
        "• 本版本只做内部记账，不包含外部地址查询/余额追踪功能"
    )


def main_menu_text():
    return (
        "📌 <b>记账机器人菜单</b>\n\n"
        "请点击下方按钮，或直接在群里输入指令。"
    )


# ================= BACKGROUND JOBS =================
async def daily_cut_loop():
    last_cut_key = "last_daily_cut"
    while True:
        try:
            enabled = str(get_setting(-1, "daily_cut_enabled", "0")) == "1"
            hour_str = get_setting(-1, "daily_cut_hour", "4") or "4"
            try:
                cut_hour = int(hour_str)
            except:
                cut_hour = 4

            if enabled:
                now = datetime.now()
                if now.hour == cut_hour:
                    last_cut = get_setting(-1, last_cut_key, "")
                    today_key = now.strftime("%Y-%m-%d")
                    if last_cut != today_key:
                        # 对所有群执行日切（如果群未禁止）
                        for chat_id, _ in get_groups():
                            if str(get_setting(chat_id, "no_daily_cut", "0")) == "1":
                                continue
                            clear_transactions(chat_id)
                            try:
                                await bot.send_message(chat_id, f"🗓 <b>日切已完成</b>\n当前账单已清空，开始新的一天。")
                            except:
                                pass

                        set_setting(-1, last_cut_key, today_key)
        except Exception as e:
            print("daily_cut_loop error:", e)

        await asyncio.sleep(60)


# ================= LIFESPAN =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_super_admin()

    # Ưu tiên URL tự có trên Render, fallback sang BASE_URL
    webhook_base = (
        os.getenv("RENDER_EXTERNAL_URL")
        or os.getenv("BASE_URL")
        or ""
    ).rstrip("/")

    webhook_url = f"{webhook_base}/webhook" if webhook_base else None
    print("webhook_url =", webhook_url)

    # Đợi app ổn định một chút rồi mới set webhook
    await asyncio.sleep(3)

    try:
        await bot.delete_webhook(drop_pending_updates=True)

        if webhook_url:
            # thử set webhook 3 lần
            last_err = None
            for i in range(3):
                try:
                    await bot.set_webhook(webhook_url)
                    print("webhook set OK")
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
                    print(f"set_webhook attempt {i+1} failed:", e)
                    await asyncio.sleep(2)

            if last_err:
                print("webhook setup error:", last_err)

    except Exception as e:
        print("webhook setup error:", e)

    task = asyncio.create_task(daily_cut_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except:
            pass

app = FastAPI(lifespan=lifespan)


# ================= START / MENU =================
@dp.message(lambda m: m.text and is_cmd(m, "/start"))
async def start_cmd(m: types.Message):
    if not is_private(m):
        return
    await m.answer(main_menu_text(), reply_markup=menu_kb())


@dp.message(lambda m: m.text == "使用说明")
async def menu_help(m: types.Message):
    await m.answer(help_text())


@dp.message(lambda m: m.text == "开始记账")
async def menu_begin(m: types.Message):
    if not is_private(m):
        return
    await m.answer(
        "📎 <b>开始记账</b>\n\n"
        "请把机器人添加进群，然后在群里输入：<b>开始</b>\n"
        "设置汇率：<b>设置汇率190</b>\n"
        "设置费率：<b>设置费率7</b>\n"
        "开始后即可输入 <b>+10000</b>、<b>-10000</b>、<b>下发5000</b> 等。",
        reply_markup=menu_kb()
    )


@dp.message(lambda m: m.text == "申请试用")
async def menu_trial(m: types.Message):
    await m.answer("✅ 试用功能请联系管理员开通。")


@dp.message(lambda m: m.text == "自助续费")
async def menu_renew(m: types.Message):
    await m.answer("🔑 自助续费请联系后台管理员。")


@dp.message(lambda m: m.text == "实时U价")
async def menu_rate(m: types.Message):
    rate = get_setting(-1, "rate", "190")
    fee = get_setting(-1, "fee", "7")
    await m.answer(f"💹 当前全局汇率：{rate}\n💰 当前全局费率：{fee}%")


@dp.message(lambda m: m.text == "管理客服")
async def menu_support(m: types.Message):
    await m.answer("👨‍💼 管理客服：请填写你自己的客服联系方式。")


@dp.message(lambda m: m.text == "地址查询")
async def menu_address_query(m: types.Message):
    await m.answer("此版本未开放外部地址查询功能。")


@dp.message(lambda m: m.text == "分组功能")
async def menu_group_func(m: types.Message):
    await m.answer(
        "👥 <b>分组功能</b>\n\n"
        "• 本版本支持按群记账\n"
        "• 同时可设置操作员\n"
        "• 也可使用全局操作员\n"
        "• 账单统计会按当前群进行"
    )


@dp.message(lambda m: m.text == "群发广播")
async def menu_broadcast(m: types.Message, state: FSMContext):
    if not m.from_user:
        return

    if is_group_message(m):
        if not is_admin_or_operator(m.chat.id, m.from_user):
            return await m.reply("❌ 无权限")
        scope = "current"
        target_chat_id = m.chat.id
    else:
        # 私聊里默认群发到所有已保存群组；超级管理员才允许
        if get_admin(m.from_user.id) != "super":
            return await m.answer("❌ 只有超级管理员可在私聊里执行全局群发。")
        scope = "all"
        target_chat_id = -1

    await state.set_state(BroadcastFSM.waiting_content)
    await state.update_data(scope=scope, target_chat_id=target_chat_id)
    await m.answer(
        "📢 <b>群发广播</b>\n\n"
        "请直接发送你要广播的内容（支持文本、图片、视频、文档等）。"
    )


# ================= ADMIN / OPERATOR =================
@dp.message(lambda m: m.text and is_cmd(m, "/addadmin", "/promote"))
async def add_admin_cmd(m: types.Message):
    if get_admin(m.from_user.id) != "super":
        return await m.reply("❌ 只有超级管理员可以添加管理员")

    uid = None
    if m.reply_to_message and m.reply_to_message.from_user:
        uid = m.reply_to_message.from_user.id
    else:
        parts = m.text.split()
        if len(parts) >= 2 and parts[1].isdigit():
            uid = int(parts[1])

    if not uid:
        return await m.reply("用法：/addadmin 123456789 或回复某人消息后输入 /addadmin")

    add_admin(uid, "admin")
    await m.reply(f"✅ 已添加管理员：{uid}")


@dp.message(lambda m: m.text and is_cmd(m, "/deladmin", "/demote"))
async def del_admin_cmd(m: types.Message):
    if get_admin(m.from_user.id) != "super":
        return await m.reply("❌ 只有超级管理员可以删除管理员")

    uid = None
    if m.reply_to_message and m.reply_to_message.from_user:
        uid = m.reply_to_message.from_user.id
    else:
        parts = m.text.split()
        if len(parts) >= 2 and parts[1].isdigit():
            uid = int(parts[1])

    if not uid:
        return await m.reply("用法：/deladmin 123456789 或回复某人消息后输入 /deladmin")

    remove_admin(uid)
    await m.reply(f"✅ 已删除管理员：{uid}")


@dp.message(lambda m: m.text and is_cmd(m, "/admins"))
async def admins_cmd(m: types.Message):
    if get_admin(m.from_user.id) not in ("super", "admin"):
        return await m.reply("❌ 无权限")

    rows = get_all_admins()
    if not rows:
        return await m.reply("暂无管理员")

    text = "📋 <b>管理员列表</b>\n\n"
    for uid, role in rows:
        text += f"• `{uid}` — {role}\n"
    await m.reply(text)


@dp.message(lambda m: m.text and is_cmd(m, "/myrole"))
async def myrole_cmd(m: types.Message):
    role = get_admin(m.from_user.id)
    await m.reply(f"👤 你的权限：{role or '无'}")


@dp.message(lambda m: m.text and ("添加操作员" in m.text or "删除操作员" in m.text or "显示操作员" in m.text))
async def operator_cmd(m: types.Message):
    if not is_group_message(m):
        return await m.reply("请在群里使用该功能。")

    ensure_group(m)
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    txt = m.text.strip()

    if "显示操作员" in txt:
        ops = get_operators(m.chat.id)
        gops = get_global_operators()
        out = "👥 <b>当前群操作员</b>\n\n"
        if ops:
            for uid, uname, role in ops:
                out += f"• {uid or ''} @{uname or ''} {role}\n"
        else:
            out += "暂无群操作员\n"

        out += "\n🌍 <b>全局操作员</b>\n"
        if gops:
            for uid, uname, role in gops:
                out += f"• {uid or ''} @{uname or ''} {role}\n"
        else:
            out += "暂无全局操作员"
        return await m.reply(out)

    add_flag = "添加操作员" in txt
    del_flag = "删除操作员" in txt

    uid = None
    uname = None
    disp = None

    if m.reply_to_message and m.reply_to_message.from_user:
        u = m.reply_to_message.from_user
        uid = u.id
        uname = u.username or ""
        disp = u.full_name
    else:
        m2 = re.search(r"@([A-Za-z0-9_]+)", txt)
        if m2:
            uname = m2.group(1)

    if add_flag:
        if uid is None and not uname:
            return await m.reply("用法：@xxxx 添加操作员，或回复某人消息后输入 添加操作员")
        add_operator(m.chat.id, user_id=uid, username=uname, role="operator")
        return await m.reply(f"✅ 已添加操作员：{disp or ('@' + uname if uname else uid)}")

    if del_flag:
        if uid is None and not uname:
            return await m.reply("用法：@xxxx 删除操作员，或回复某人消息后输入 删除操作员")
        remove_operator(m.chat.id, user_id=uid, username=uname)
        return await m.reply(f"✅ 已删除操作员：{disp or ('@' + uname if uname else uid)}")


@dp.message(lambda m: m.text and ("全局操作人" in m.text or "全局记员" in m.text or "全部记员" in m.text))
async def global_operator_cmd(m: types.Message):
    if not is_private(m) and not is_group_message(m):
        return
    if get_admin(m.from_user.id) != "super":
        return await m.reply("❌ 只有超级管理员可以设置全局操作员")

    txt = m.text.strip()

    if "显示全局操作人" in txt:
        rows = get_global_operators()
        if not rows:
            return await m.reply("暂无全局操作员")
        out = "🌍 <b>全局操作员</b>\n\n"
        for uid, uname, role in rows:
            out += f"• {uid or ''} @{uname or ''} {role}\n"
        return await m.reply(out)

    if "删除所有人操作员" in txt:
        clear_operators(-1)
        return await m.reply("✅ 已清空全局操作员")

    if "全部记员" in txt and "设置" in txt:
        # 这里按“当前群已知成员”批量设置
        if not is_group_message(m):
            return await m.reply("请在群里使用此命令")
        members = get_members(m.chat.id)
        for _, uid, uname, name, last_seen in members:
            try:
                add_operator(m.chat.id, user_id=uid, username=uname, role="operator")
            except:
                pass
        return await m.reply("✅ 已将当前群已记录成员设置为操作员")

    if "全部记员" in txt and ("删除" in txt or "取消" in txt):
        if not is_group_message(m):
            return await m.reply("请在群里使用此命令")
        clear_operators(m.chat.id)
        return await m.reply("✅ 已清空当前群操作员")


# ================= CONFIG =================
@dp.message(lambda m: m.text and (
    m.text.startswith("设置汇率") or m.text.startswith("配置汇率") or
    m.text.startswith("设置费率") or m.text.startswith("配置费率") or
    m.text.startswith("单笔手续费") or m.text.startswith("代付费率") or
    m.text.startswith("代付汇率") or m.text.startswith("设置火币汇率") or
    m.text.startswith("设置欧易汇率") or m.text.startswith("设置实时汇率") or
    m.text.startswith("删除手续费") or m.text.startswith("删除代付费率") or
    m.text.startswith("删除代付汇率")
))
async def config_cmd(m: types.Message):
    if not is_group_message(m):
        # 也允许私聊改全局配置
        chat_id = -1
    else:
        ensure_group(m)
        chat_id = m.chat.id

    if not is_admin_or_operator(chat_id if chat_id != -1 else -1, m.from_user):
        return await m.reply("❌ 无权限")

    txt = m.text.strip()

    def get_num(pattern):
        mm = re.search(pattern, txt)
        return mm.group(1) if mm else None

    if txt.startswith("设置汇率"):
        v = get_num(r"设置汇率\s*(-?\d+(?:\.\d+)?)")
        if not v:
            return await m.reply("用法：设置汇率190")
        set_chat_setting(chat_id, "rate", v)
        return await m.reply(f"✅ 设置成功，当前汇率：{v}")

    if txt.startswith("配置汇率"):
        v = get_num(r"配置汇率\s*(-?\d+(?:\.\d+)?)")
        if not v:
            return await m.reply("用法：配置汇率8.5")
        # 这里作为“微调值”保存
        set_chat_setting(chat_id, "rate_adjust", v)
        return await m.reply(f"✅ 配置汇率微调成功：{v}")

    if txt.startswith("设置费率") or txt.startswith("配置费率"):
        v = get_num(r"(?:设置费率|配置费率)\s*(-?\d+(?:\.\d+)?)")
        if v is None:
            return await m.reply("用法：设置费率7")
        set_chat_setting(chat_id, "fee", v)
        return await m.reply(f"✅ 设置成功，当前费率：{v}%")

    if txt.startswith("单笔手续费"):
        v = get_num(r"单笔手续费\s*(-?\d+(?:\.\d+)?)")
        if v is None:
            return await m.reply("用法：单笔手续费20")
        set_chat_setting(chat_id, "single_fee", v)
        return await m.reply(f"✅ 设置单笔手续费：{v}")

    if txt.startswith("代付费率"):
        v = get_num(r"代付费率\s*(-?\d+(?:\.\d+)?)")
        if v is None:
            return await m.reply("用法：代付费率-5")
        set_chat_setting(chat_id, "pay_fee", v)
        return await m.reply(f"✅ 设置代付费率：{v}")

    if txt.startswith("代付汇率"):
        v = get_num(r"代付汇率\s*(-?\d+(?:\.\d+)?)")
        if v is None:
            return await m.reply("用法：代付汇率8")
        set_chat_setting(chat_id, "pay_rate", v)
        return await m.reply(f"✅ 设置代付汇率：{v}")

    if txt.startswith("设置火币汇率"):
        v = get_num(r"设置火币汇率\s*(-?\d+(?:\.\d+)?)")
        if v is not None:
            set_chat_setting(chat_id, "huobi_rate", v)
            return await m.reply(f"✅ 火币汇率：{v}")
        return await m.reply("用法：设置火币汇率190")

    if txt.startswith("设置欧易汇率"):
        v = get_num(r"设置欧易汇率\s*(-?\d+(?:\.\d+)?)")
        if v is not None:
            set_chat_setting(chat_id, "okx_rate", v)
            return await m.reply(f"✅ 欧易汇率：{v}")
        return await m.reply("用法：设置欧易汇率190")

    if txt.startswith("设置实时汇率"):
        v = get_num(r"设置实时汇率\s*(-?\d+(?:\.\d+)?)")
        if v is not None:
            set_chat_setting(chat_id, "real_rate", v)
            return await m.reply(f"✅ 实时汇率：{v}")
        return await m.reply("用法：设置实时汇率190")

    if txt.startswith("删除手续费"):
        delete_setting(chat_id, "single_fee")
        return await m.reply("✅ 已删除手续费配置")

    if txt.startswith("删除代付费率"):
        delete_setting(chat_id, "pay_fee")
        return await m.reply("✅ 已删除代付费率配置")

    if txt.startswith("删除代付汇率"):
        delete_setting(chat_id, "pay_rate")
        return await m.reply("✅ 已删除代付汇率配置")


@dp.message(lambda m: m.text and (m.text in ("费率", "配置", "查看配置")))
async def show_config_cmd(m: types.Message):
    chat_id = m.chat.id if is_group_message(m) else -1
    rate = get_chat_setting(chat_id, "rate", "190")
    fee = get_chat_setting(chat_id, "fee", "7")
    single_fee = get_chat_setting(chat_id, "single_fee", "未设置")
    pay_fee = get_chat_setting(chat_id, "pay_fee", "未设置")
    pay_rate = get_chat_setting(chat_id, "pay_rate", "未设置")
    enabled = get_chat_setting(chat_id, "enabled", "0")
    cut_hour = get_chat_setting(chat_id, "daily_cut_hour", "4")
    cut_enabled = get_chat_setting(chat_id, "daily_cut_enabled", "0")
    no_daily_cut = get_chat_setting(chat_id, "no_daily_cut", "0")

    await m.reply(
        f"⚙️ <b>当前配置</b>\n\n"
        f"固定汇率：{rate}\n"
        f"当前费率：{fee}%\n"
        f"单笔手续费：{single_fee}\n"
        f"代付费率：{pay_fee}\n"
        f"代付汇率：{pay_rate}\n"
        f"记账状态：{'开启' if str(enabled) == '1' else '关闭'}\n"
        f"日切时间：{cut_hour}:00\n"
        f"日切开关：{'开启' if str(cut_enabled) == '1' else '关闭'}\n"
        f"禁止日切：{'是' if str(no_daily_cut) == '1' else '否'}"
    )


# ================= START / STOP / CHAT PERMISSION =================
@dp.message(lambda m: m.text and m.text in ("开始", "开始记账", "开启记账"))
async def start_accounting(m: types.Message):
    if not is_group_message(m):
        return
    ensure_group(m)
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    set_chat_setting(m.chat.id, "enabled", "1")
    await m.reply("✅ 记账已开启！")


@dp.message(lambda m: m.text and m.text in ("关闭记账", "停止记账"))
async def stop_accounting(m: types.Message):
    if not is_group_message(m):
        return
    ensure_group(m)
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    set_chat_setting(m.chat.id, "enabled", "0")
    await m.reply("⛔ 记账已关闭！")


@dp.message(lambda m: m.text and m.text in ("上课", "下课"))
async def group_permission_cmd(m: types.Message):
    if not is_group_message(m):
        return
    ensure_group(m)
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    try:
        if m.text == "上课":
            await bot.set_chat_permissions(
                m.chat.id,
                permissions=types.ChatPermissions(can_send_messages=True)
            )
            await m.reply("✅ 已开启发言")
        else:
            await bot.set_chat_permissions(
                m.chat.id,
                permissions=types.ChatPermissions(can_send_messages=False)
            )
            await m.reply("✅ 已禁言")
    except Exception as e:
        await m.reply("❌ 机器人没有权限修改群权限")
        print("group_permission_cmd error:", e)


# ================= LOCKED ROLES =================
@dp.message(lambda m: m.text and m.text.startswith("锁定记账"))
async def lock_income_cmd(m: types.Message):
    if not is_group_message(m):
        return
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    if not m.reply_to_message or not m.reply_to_message.from_user:
        return await m.reply("请回复指定人消息后输入：锁定记账")

    u = m.reply_to_message.from_user
    set_chat_setting(m.chat.id, "locked_income_user_id", str(u.id))
    set_chat_setting(m.chat.id, "locked_income_username", u.username or "")
    await m.reply(f"✅ 已锁定记账：{u.full_name}")


@dp.message(lambda m: m.text and m.text.startswith("锁定下发"))
async def lock_payout_cmd(m: types.Message):
    if not is_group_message(m):
        return
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    if not m.reply_to_message or not m.reply_to_message.from_user:
        return await m.reply("请回复指定人消息后输入：锁定下发")

    u = m.reply_to_message.from_user
    set_chat_setting(m.chat.id, "locked_payout_user_id", str(u.id))
    set_chat_setting(m.chat.id, "locked_payout_username", u.username or "")
    await m.reply(f"✅ 已锁定下发：{u.full_name}")


@dp.message(lambda m: m.text and m.text.startswith("锁定查帐"))
async def lock_query_cmd(m: types.Message):
    if not is_group_message(m):
        return
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    if not m.reply_to_message or not m.reply_to_message.from_user:
        return await m.reply("请回复指定人消息后输入：锁定查帐")

    u = m.reply_to_message.from_user
    set_chat_setting(m.chat.id, "locked_query_user_id", str(u.id))
    set_chat_setting(m.chat.id, "locked_query_username", u.username or "")
    await m.reply(f"✅ 已锁定查账：{u.full_name}")


# ================= DAILY CUT =================
@dp.message(lambda m: m.text and m.text.startswith("设置日切"))
async def set_daily_cut_cmd(m: types.Message):
    if not is_group_message(m):
        chat_id = -1
    else:
        chat_id = m.chat.id

    if not is_admin_or_operator(chat_id, m.from_user):
        return await m.reply("❌ 无权限")

    mm = re.search(r"设置日切\s*(\d{1,2})", m.text)
    if not mm:
        return await m.reply("用法：设置日切04")

    hour = int(mm.group(1))
    set_chat_setting(chat_id, "daily_cut_hour", str(hour))
    set_chat_setting(chat_id, "daily_cut_enabled", "1")
    await m.reply(f"✅ 日切已设置为 {hour:02d}:00")


@dp.message(lambda m: m.text == "关闭日切")
async def close_daily_cut_cmd(m: types.Message):
    chat_id = m.chat.id if is_group_message(m) else -1
    if not is_admin_or_operator(chat_id, m.from_user):
        return await m.reply("❌ 无权限")
    set_chat_setting(chat_id, "daily_cut_enabled", "0")
    await m.reply("✅ 已关闭日切")


@dp.message(lambda m: m.text == "禁止日切")
async def prohibit_daily_cut_cmd(m: types.Message):
    if not is_group_message(m):
        return
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")
    set_chat_setting(m.chat.id, "no_daily_cut", "1")
    await m.reply("✅ 当前群已禁止日切")


# ================= RESET / UNDO / REPORT =================
@dp.message(lambda m: m.text and m.text in ("重置", "清零", "删除账单", "结束账单"))
async def reset_ledger_cmd(m: types.Message):
    if not is_group_message(m):
        return
    ensure_group(m)
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    clear_transactions(m.chat.id)
    await m.reply("✅ 账单已清空")


@dp.message(lambda m: m.text and m.text in ("撤销", "撤销账单"))
async def undo_cmd(m: types.Message):
    if not is_group_message(m):
        return
    ensure_group(m)
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    if m.reply_to_message:
        # 如果回复到某条账单消息，尝试从文本里找 ID（如果有）
        m2 = re.search(r"#TX:(\d+)", m.reply_to_message.text or "")
        if m2:
            tx_id = int(m2.group(1))
            undo_transaction(tx_id)
            return await m.reply(f"✅ 已撤销账单 ID: {tx_id}")

    last = get_last_transaction(m.chat.id)
    if not last:
        return await m.reply("暂无可撤销记录")

    undo_transaction(last[0])
    await m.reply(f"✅ 已撤销上一笔账单 ID: {last[0]}")


@dp.message(lambda m: m.text and m.text in ("账单", "/我", "我的账单"))
async def my_bill_cmd(m: types.Message):
    if not is_group_message(m):
        return
    ensure_group(m)

    keyword = None
    target_user_id = m.from_user.id

    # “我的账单”仅操作员
    if m.text == "我的账单" and not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 仅操作员可查看“我的账单”")

    # 如果回复某人消息，则看被回复人的账单
    if m.reply_to_message and m.reply_to_message.from_user:
        target_user_id = m.reply_to_message.from_user.id

    txs = get_transactions(m.chat.id, user_id=target_user_id)
    stats = summarize_transactions(txs)
    await m.reply(
        f"📒 <b>我的账单</b>\n\n"
        f"入款：{len([t for t in txs if t[6]=='income'])} 笔\n"
        f"下发：{len([t for t in txs if t[6]=='payout'])} 笔\n"
        f"总入款：{fmt_num(stats['total_income_unit'])}U\n"
        f"已下发：{fmt_num(stats['paid'])}U\n"
        f"未下发：{fmt_num(stats['pending'])}U"
    )


@dp.message(lambda m: m.text and m.text.endswith(" 账单"))
async def search_bill_cmd(m: types.Message):
    if not is_group_message(m):
        return
    ensure_group(m)

    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    kw = m.text[:-3].strip()
    if not kw:
        return

    txs = get_transactions(m.chat.id, keyword=kw)
    if not txs:
        return await m.reply("暂无匹配账单")

    text = f"🔎 <b>{kw} 的账单</b>\n\n"
    for tx in txs[:20]:
        text += format_tx_line(tx) + "\n"
    await m.reply(text)


@dp.message(lambda m: m.text and m.text in ("总账单", "完整账单"))
async def full_report_cmd(m: types.Message):
    if not is_group_message(m):
        return
    ensure_group(m)
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    start_ts, end_ts = day_range()
    text = report_text(m.chat.id, start_ts, end_ts, title="今日总账")
    await m.reply(text, reply_markup=report_kb())


@dp.message(lambda m: m.text and m.text in ("上个月总账单", "上月总账"))
async def last_month_report_cmd(m: types.Message):
    if not is_group_message(m):
        return
    ensure_group(m)
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    start_ts, end_ts = month_range(1)
    text = report_text(m.chat.id, start_ts, end_ts, title="上月总账")
    await m.reply(text, reply_markup=report_kb())


@dp.message(lambda m: m.text and m.text.startswith("账单汇率"))
async def bill_rate_fix_cmd(m: types.Message):
    # 示例：账单汇率8更新8.5
    if not is_group_message(m):
        return
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")
    await m.reply("✅ 账单汇率修正功能已预留。若你要，我可以继续补上“按账单ID批量改汇率”的版本。")


# ================= BROADCAST FSM =================
@dp.message(BroadcastFSM.waiting_content)
async def broadcast_receive_content(m: types.Message, state: FSMContext):
    data = await state.get_data()
    scope = data.get("scope", "current")
    target_chat_id = data.get("target_chat_id", m.chat.id)

    await state.update_data(
        source_chat_id=m.chat.id,
        source_message_id=m.message_id,
        source_has_media=bool(m.photo or m.video or m.document or m.audio or m.voice),
        scope=scope,
        target_chat_id=target_chat_id
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="确认群发(普通)", callback_data="bc:copy"),
            InlineKeyboardButton(text="确认群发(转发)", callback_data="bc:fwd"),
        ],
        [
            InlineKeyboardButton(text="取消群发", callback_data="bc:cancel")
        ]
    ])

    await m.reply("请确认广播方式：", reply_markup=kb)
    await state.set_state(BroadcastFSM.waiting_confirm)


@dp.callback_query(lambda c: c.data and c.data.startswith("bc:"))
async def broadcast_callback(c: types.CallbackQuery, state: FSMContext):
    if not c.from_user:
        return
    data = await state.get_data()
    scope = data.get("scope", "current")
    source_chat_id = data.get("source_chat_id")
    source_message_id = data.get("source_message_id")

    if c.data == "bc:cancel":
        await state.clear()
        return await c.message.edit_text("✅ 已取消群发")

    if c.data not in ("bc:copy", "bc:fwd"):
        return

    if scope == "all":
        targets = [g[0] for g in get_groups()]
    else:
        target_chat_id = data.get("target_chat_id")
        targets = [target_chat_id]

    if not source_chat_id or not source_message_id:
        await state.clear()
        return await c.message.edit_text("❌ 广播内容已失效，请重新发送。")

    ok = 0
    fail = 0
    for chat_id in targets:
        try:
            if c.data == "bc:copy":
                await bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=source_chat_id,
                    message_id=source_message_id
                )
            else:
                await bot.forward_message(
                    chat_id=chat_id,
                    from_chat_id=source_chat_id,
                    message_id=source_message_id
                )
            ok += 1
        except Exception as e:
            fail += 1
            print("broadcast error:", e)

    await state.clear()
    await c.message.edit_text(f"✅ 群发完成\n成功：{ok}\n失败：{fail}")


# ================= USER JOIN =================
@dp.message(lambda m: m.new_chat_members)
async def new_members(m: types.Message):
    ensure_group(m)
    text = (
        f"欢迎 {', '.join([u.full_name for u in m.new_chat_members])} 来到本群。\n"
        "记账机器人已就绪。"
    )
    try:
        await m.reply(text)
    except:
        pass


# ================= MY_CHAT_MEMBER =================
@dp.my_chat_member()
async def on_bot_member_update(e: types.ChatMemberUpdated):
    try:
        if e.new_chat_member.status in ("member", "administrator") and e.old_chat_member.status == "left":
            save_group(e.chat.id, e.chat.title or "Unnamed group")
            await bot.send_message(e.chat.id, "✅ 记账机器人已加入本群。")
    except Exception as ex:
        print("on_bot_member_update error:", ex)


# ================= LEDGER PARSER =================
@dp.message()
async def ledger_handler(m: types.Message):
    if not is_group_message(m):
        return
    if not m.text:
        return
    if m.text.startswith("/"):
        return

    ensure_group(m)

    # 只在记账开启时处理
    if not get_enabled(m.chat.id):
        return

    txt = m.text.strip()

    # 允许普通成员自己查自己的账单
    if txt in ("账单", "/我"):
        return

    # 锁定记账/下发/查账
    locked_income_uid = get_chat_setting(m.chat.id, "locked_income_user_id", None)
    locked_payout_uid = get_chat_setting(m.chat.id, "locked_payout_user_id", None)
    locked_query_uid = get_chat_setting(m.chat.id, "locked_query_user_id", None)

    # ---------- 1) P+ / P- 寄存 ----------
    if txt.startswith("P+") or txt.startswith("P-"):
        if locked_income_uid and str(m.from_user.id) != str(locked_income_uid):
            return await m.reply("❌ 当前已锁定记账，非锁定人无法操作")

        parsed = parse_amount_expr(txt[1:], m.chat.id, default_direct_unit=True)
        if not parsed:
            return await m.reply("❌ 格式错误")

        unit_amount = parsed["unit_amount"]
        target = None

        # target name for display
        if m.reply_to_message and m.reply_to_message.from_user:
            target = m.reply_to_message.from_user.full_name

        tx_id = add_transaction(
            chat_id=m.chat.id,
            user_id=m.from_user.id,
            username=m.from_user.username or "",
            display_name=m.from_user.full_name,
            target_name=target,
            kind="reserve",
            raw_amount=parsed["raw_amount"],
            unit_amount=unit_amount,
            rate_used=parsed["rate_used"],
            fee_used=parsed["fee_used"],
            note="寄存",
            original_text=txt
        )

        await m.reply(f"✅ 已记录寄存：{fmt_num(unit_amount)}U\n#TX:{tx_id}")
        start_ts, end_ts = day_range()
        await m.reply(report_text(m.chat.id, start_ts, end_ts, title="今日账单"), reply_markup=report_kb())
        return

    # ---------- 2) 下发 ----------
    if txt.startswith("下发"):
        if locked_payout_uid and str(m.from_user.id) != str(locked_payout_uid):
            return await m.reply("❌ 当前已锁定下发，非锁定人无法操作")

        body = txt[len("下发"):].strip()
        if not body:
            return await m.reply("格式：下发5000 / 下发-2000 / 下发1000R / 下发1000/7.8")

        # 默认：直接U
        # 如果带 R 或 /rate 或 *fee% -> 按换算
        has_conversion = ("R" in body) or ("r" in body) or ("/" in body) or ("*" in body)
        expr = body.replace("R", "").replace("r", "")
        parsed = parse_amount_expr(expr, m.chat.id, default_direct_unit=not has_conversion)
        if not parsed:
            return await m.reply("❌ 下发格式错误")

        unit_amount = parsed["unit_amount"]
        target = None
        if m.reply_to_message and m.reply_to_message.from_user:
            target = m.reply_to_message.from_user.full_name

        tx_id = add_transaction(
            chat_id=m.chat.id,
            user_id=m.from_user.id,
            username=m.from_user.username or "",
            display_name=m.from_user.full_name,
            target_name=target,
            kind="payout",
            raw_amount=parsed["raw_amount"],
            unit_amount=unit_amount,
            rate_used=parsed["rate_used"],
            fee_used=parsed["fee_used"],
            note="下发",
            original_text=txt
        )

        await m.reply(f"💸 又没了～\n#TX:{tx_id}")
        start_ts, end_ts = day_range()
        await m.reply(report_text(m.chat.id, start_ts, end_ts, title="今日账单"), reply_markup=report_kb())
        return

    # ---------- 3) 普通 +/- 记账 ----------
    # 允许：张三+1000 / 张三 +1000 / +1000 备注 / +1000u / +10000/7.8*12%
    target_name, body = split_target_prefix(txt)

    # 0 用于显示报表
    if body in ("+0", "-0", "0"):
        start_ts, end_ts = day_range()
        await m.reply(report_text(m.chat.id, start_ts, end_ts, title="今日账单"), reply_markup=report_kb())
        return

    # 允许“记账备注”形式：+1000 备注
    note = ""
    if " " in body:
        first_part, note = body.split(" ", 1)
        amount_expr = first_part.strip()
        note = note.strip()
    else:
        amount_expr = body.strip()

    if not amount_expr or amount_expr[0] not in ("+", "-"):
        return

    # 记账锁定
    if locked_income_uid and str(m.from_user.id) != str(locked_income_uid):
        return await m.reply("❌ 当前已锁定记账，非锁定人无法操作")

    parsed = parse_amount_expr(amount_expr, m.chat.id, default_direct_unit=False)
    if not parsed:
        return await m.reply("❌ 记账格式错误")

    # 如果是 +7777u 也会走这里，unit_amount 直接保存
    unit_amount = parsed["unit_amount"]
    kind = "income" if amount_expr.startswith("+") else "payout"

    # target_name 优先：命令前缀
    if not target_name:
        if m.reply_to_message and m.reply_to_message.from_user:
            target_name = m.reply_to_message.from_user.full_name
        else:
            target_name = ""

    tx_id = add_transaction(
        chat_id=m.chat.id,
        user_id=m.from_user.id,
        username=m.from_user.username or "",
        display_name=m.from_user.full_name,
        target_name=target_name,
        kind=kind,
        raw_amount=parsed["raw_amount"],
        unit_amount=unit_amount,
        rate_used=parsed["rate_used"],
        fee_used=parsed["fee_used"],
        note=note,
        original_text=txt
    )

    if kind == "income":
        ack = "💰 来钱了～"
    else:
        ack = "💸 又没了～"

    await m.reply(f"{ack}\n#TX:{tx_id}")
    start_ts, end_ts = day_range()
    await m.reply(report_text(m.chat.id, start_ts, end_ts, title="今日账单"), reply_markup=report_kb())


# ================= REPORT BUTTON =================
@dp.callback_query(lambda c: c.data == "report:full")
async def report_full_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return
    if not is_group_message(c.message):
        return
    if not is_admin_or_operator(c.message.chat.id, c.from_user):
        return await c.answer("无权限", show_alert=True)

    start_ts, end_ts = day_range()
    await c.message.reply(report_text(c.message.chat.id, start_ts, end_ts, title="今日账单"), reply_markup=report_kb())
    await c.answer()


# ================= GENERAL QUERY / CONFIG HELP =================
@dp.message(lambda m: m.text and m.text == "管理客服")
async def show_support(m: types.Message):
    await m.reply("👨‍💼 管理客服：请在这里填你的客服联系方式。")


@dp.message(lambda m: m.text and m.text == "群发广播")
async def menu_broadcast_text(m: types.Message, state: FSMContext):
    # 复用上面的广播逻辑
    if is_private(m):
        if get_admin(m.from_user.id) != "super":
            return await m.answer("❌ 只有超级管理员可在私聊里全局群发。")
        scope = "all"
        target_chat_id = -1
    else:
        ensure_group(m)
        if not is_admin_or_operator(m.chat.id, m.from_user):
            return await m.reply("❌ 无权限")
        scope = "current"
        target_chat_id = m.chat.id

    await state.set_state(BroadcastFSM.waiting_content)
    await state.update_data(scope=scope, target_chat_id=target_chat_id)
    await m.reply("📢 请发送要广播的内容。")


# ================= WEBHOOK =================
@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}


@app.get("/")
def home():
    return {"status": "running"}


# ================= RUN =================
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT)
