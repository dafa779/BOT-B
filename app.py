import os
import re
import time
import asyncio
import requests
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
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
    BufferedInputFile,
)
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv
import uvicorn

from db import (
    init_db,
    get_setting,
    set_setting,
    delete_setting,
    set_button_config,
    get_all_button_configs,
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
    get_last_transaction,
    add_wallet_check,
    get_wallet_checks_page,
    count_wallet_checks,
    undo_transaction,
    clear_transactions,
    get_transactions,
    set_trial_code,
    get_trial_code,
    add_access_user,
    remove_access_user,
    has_access_user,
    get_access_users,
    get_expired_access_users,
)

# ================= ENV =================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "8080"))
SUPER_ADMIN_ID = int(os.getenv("SUPER_ADMIN_ID", "0") or 0)

BASE_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("BASE_URL") or "").rstrip("/")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing in environment variables")

# ================= BOT =================
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(link_preview_is_disabled=True)
)
dp = Dispatcher(storage=MemoryStorage())

BOT_USERNAME = None

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


class TrialFSM(StatesGroup):
    waiting_code = State()
    waiting_create_code = State()


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


def should_ignore_message(m: types.Message):
    return (
        not m
        or not m.from_user
        or m.from_user.is_bot
        or not m.text
    )


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


TRON_ADDR_RE = re.compile(r"\bT[1-9A-HJ-NP-Za-km-z]{33}\b")
USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"


def extract_tron_address(text: str):
    if not text:
        return None
    m = TRON_ADDR_RE.search(text.strip())
    return m.group(0) if m else None


def fmt_ts(ts):
    if not ts:
        return "-"
    try:
        ts = int(ts)
        if ts > 10_000_000_000:
            ts = ts // 1000
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except:
        return "-"


def fmt_wallet_ts(ts):
    return fmt_ts(ts)


def _pick_account(payload):
    if not isinstance(payload, dict):
        return None

    if isinstance(payload.get("data"), list) and payload["data"]:
        return payload["data"][0]

    if payload.get("address"):
        return payload

    if isinstance(payload.get("data"), dict):
        return payload["data"]

    return None


def _parse_trc20_usdt(account):
    if not isinstance(account, dict):
        return None

    candidates = [
        "trc20token_balances",
        "trc20",
        "tokenBalances",
        "tokens",
        "assetV2",
    ]

    for key in candidates:
        items = account.get(key)
        if not items:
            continue

        if isinstance(items, dict):
            items = [items]

        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue

            sym = str(
                item.get("tokenAbbr")
                or item.get("symbol")
                or item.get("tokenName")
                or item.get("name")
                or ""
            ).upper()

            contract = str(
                item.get("contract_address")
                or item.get("tokenAddress")
                or item.get("tokenId")
                or item.get("contract")
                or ""
            )

            if sym == "USDT" or contract == USDT_CONTRACT:
                raw = (
                    item.get("balance")
                    or item.get("value")
                    or item.get("amount")
                    or item.get("tokenValue")
                )
                if raw is None:
                    return 0

                try:
                    decimals = int(item.get("precision") or item.get("decimals") or 6)
                except:
                    decimals = 6

                try:
                    return float(raw) / (10 ** decimals)
                except:
                    try:
                        return float(raw)
                    except:
                        return 0

    return None


async def check_tron_address(address: str):
    def _fetch():
        headers = {
            "accept": "application/json",
            "user-agent": "Mozilla/5.0",
        }

        try:
            url = f"https://api.trongrid.io/v1/accounts/{address}"
            r = requests.get(url, timeout=15, headers=headers)
            if r.ok:
                payload = r.json()
                acc = _pick_account(payload)
                if acc:
                    return {"source": "trongrid", "account": acc}
        except:
            pass

        try:
            url = f"https://apilist.tronscanapi.com/api/account?address={address}"
            r = requests.get(url, timeout=15, headers=headers)
            if r.ok:
                payload = r.json()
                acc = _pick_account(payload)
                if acc:
                    return {"source": "tronscan", "account": acc}
        except:
            pass

        return None

    result = await asyncio.to_thread(_fetch)
    if not result:
        return None

    acc = result["account"]

    trx_balance = None
    try:
        if acc.get("balance") is not None:
            trx_balance = float(acc.get("balance")) / 1_000_000
    except:
        trx_balance = None

    usdt_balance = _parse_trc20_usdt(acc)

    tx_count = (
        acc.get("transaction_count")
        or acc.get("txCount")
        or acc.get("transactionsCount")
        or acc.get("totalTransactionCount")
        or acc.get("trxCount")
        or None
    )
    try:
        tx_count = int(tx_count) if tx_count is not None else None
    except:
        tx_count = None

    create_time = (
        acc.get("create_time")
        or acc.get("createTime")
        or acc.get("create_time_ms")
        or acc.get("createTimeMs")
    )

    latest_time = (
        acc.get("latest_opration_time")
        or acc.get("latestOperationTime")
        or acc.get("latest_operation_time")
        or acc.get("latest_tx_time")
    )

    return {
        "source": result["source"],
        "address": address,
        "trx_balance": trx_balance,
        "usdt_balance": usdt_balance,
        "tx_count": tx_count,
        "create_time": create_time,
        "latest_time": latest_time,
        "raw": acc,
    }


def make_wallet_card_image(
    address,
    sender_name,
    trx_balance=None,
    usdt_balance=None,
    tx_count=None,
    source="trongrid",
    create_time=None,
    latest_time=None
):
    width, height = 1080, 1350

    top_green = (18, 185, 150)
    top_green2 = (16, 165, 138)
    body_bg = (20, 30, 44)
    panel_bg = (26, 40, 58)
    panel_bg2 = (30, 46, 66)
    white = (245, 248, 250)
    mute = (165, 180, 190)
    gold = (245, 198, 76)
    blue = (120, 185, 255)
    green = (100, 235, 160)
    red = (255, 120, 120)

    img = Image.new("RGB", (width, height), body_bg)
    draw = ImageDraw.Draw(img)

    for y in range(height):
        if y < 330:
            r = int(top_green[0] * (1 - y / 330) + top_green2[0] * (y / 330))
            g = int(top_green[1] * (1 - y / 330) + top_green2[1] * (y / 330))
            b = int(top_green[2] * (1 - y / 330) + top_green2[2] * (y / 330))
            draw.line([(0, y), (width, y)], fill=(r, g, b))
        else:
            draw.line([(0, y), (width, y)], fill=body_bg)

    font_candidates = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKSC-Regular.otf",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]

    def load_font(size):
        for fp in font_candidates:
            try:
                return ImageFont.truetype(fp, size)
            except:
                pass
        return ImageFont.load_default()

    font_title = load_font(54)
    font_sub = load_font(28)
    font_mid = load_font(32)
    font_small = load_font(24)

    def box(x1, y1, x2, y2, radius=26, fill=panel_bg, outline=None, width=2):
        draw.rounded_rectangle((x1, y1, x2, y2), radius=radius, fill=fill, outline=outline, width=width)

    def text(x, y, s, font, fill=white):
        draw.text((x, y), str(s), font=font, fill=fill)

    def center_text(y, s, font, fill=white):
        bbox = draw.textbbox((0, 0), str(s), font=font)
        w = bbox[2] - bbox[0]
        x = (width - w) // 2
        draw.text((x, y), str(s), font=font, fill=fill)

    def fmt_time_local(ts):
        if not ts:
            return "N/A"
        try:
            ts = int(ts)
            if ts > 10_000_000_000:
                ts = ts // 1000
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        except:
            return "N/A"

    box(40, 35, 1040, 300, radius=36, fill=top_green2, outline=(255, 255, 255, 40), width=2)

    draw.rounded_rectangle((65, 66, 170, 168), radius=24, fill=(255, 255, 255, 36), outline=(255, 255, 255, 80), width=2)
    center_text(96, "USDT", load_font(30), fill=(14, 72, 62))

    center_text(70, "USDT防篡改验证核对", font_title, fill=white)
    center_text(144, "《请双方谨慎核对地址是否与图中一致，如有误停止付款》", font_sub, fill=(232, 247, 242))

    box(90, 198, 990, 250, radius=18, fill=(60, 130, 108), outline=(220, 255, 240), width=2)
    center_text(209, address, font_mid, fill=white)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    center_text(258, f"Now: {now_str}", font_small, fill=(225, 245, 240))

    box(40, 330, 1040, 1140, radius=34, fill=panel_bg, outline=(42, 70, 90), width=2)

    text(70, 360, "🔎 查询地址：", font_mid, fill=white)
    text(250, 360, address, font_mid, fill=blue)

    text(70, 408, "📌 当前页码：第 1 页", font_mid, fill=white)

    box(60, 460, 1020, 1030, radius=28, fill=panel_bg2, outline=(55, 90, 110), width=2)

    tx_status = "已签名地址" if (tx_count or 0) > 0 else "未签名地址"
    tx_status_color = green if (tx_count or 0) > 0 else red

    rows = [
        ("💡 交易次数", str(tx_count if tx_count is not None else "N/A"), white),
        ("⏰ 首次交易", fmt_time_local(create_time), white),
        ("🌟 最后活跃", fmt_time_local(latest_time), white),
        ("🛡 签名状态", tx_status, tx_status_color),
        ("🔋 能量", "剩余：0 / 0", white),
        ("🌈 带宽", "剩余：600 / 600", white),
        ("💰 USDT 余额", f"{fmt_num(usdt_balance)} USDT", gold),
        ("💰 TRX 余额", f"{fmt_num(trx_balance)} TRX", gold),
        ("📡 数据来源", str(source), mute),
    ]

    y = 500
    gap = 58
    for label, value, value_color in rows:
        text(85, y, f"{label}：", font_mid, fill=white)
        text(330, y, value, font_mid, fill=value_color)
        y += gap

    box(60, 1055, 1020, 1125, radius=22, fill=(18, 28, 40), outline=(55, 90, 110), width=2)
    text(85, 1077, "⚠ 请务必仔细核对地址信息，确认无误后再继续操作。", font_sub, fill=gold)

    bio = BytesIO()
    img.save(bio, "PNG")
    bio.seek(0)
    return BufferedInputFile(bio.read(), filename="usdt_check_cn.png")


def get_chat_setting(chat_id, key, default=None):
    v = get_setting(chat_id, key, None)
    if v is None and chat_id != -1:
        v = get_setting(-1, key, None)
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


def has_bot_access(user_id):
    return get_admin(user_id) in ("super", "admin") or has_access_user(user_id)


def parse_block_fields(body: str):
    data = {}
    current_field = None

    for raw_line in (body or "").splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            continue

        m = re.match(r"^([A-Za-z_0-9\u4e00-\u9fff]+)\s*[:：]\s*(.*)$", line)
        if m:
            current_field = m.group(1).strip()
            data[current_field] = m.group(2).rstrip()
        else:
            if current_field:
                data[current_field] = (data.get(current_field, "") + "\n" + line).rstrip("\n")

    return data


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
                KeyboardButton(text="地址查询"),
                KeyboardButton(text="管理客服"),
            ],
            [
                KeyboardButton(text="总账单"),
                KeyboardButton(text="账单"),
                KeyboardButton(text="撤销"),
            ],
        ],
        resize_keyboard=True
    )


def start_inline_kb(user_id=None):
    if BOT_USERNAME:
        add_url = f"https://t.me/{BOT_USERNAME}?startgroup=add"
    else:
        add_url = "https://t.me/"

    buttons = [
        [InlineKeyboardButton(text="➕ 添加机器人到群", url=add_url)]
    ]

    if user_id == SUPER_ADMIN_ID:
        buttons.append([
            InlineKeyboardButton(text="🔑 创建激活码", callback_data="trial:create_code")
        ])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def report_kb(chat_id):
    buttons = get_all_button_configs(chat_id)
    if not buttons:
        buttons = get_all_button_configs(-1)

    rows = []
    if buttons:
        row = []
        for text, url in buttons:
            row.append(InlineKeyboardButton(text=text, url=url))
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)

    rows.append([InlineKeyboardButton(text="📘 完整账单", callback_data="report:full")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def send_long_text(chat_id, text, reply_markup=None):
    text = text or ""
    chunks = [text[i:i + 3500] for i in range(0, len(text), 3500)] or [""]
    for i, chunk in enumerate(chunks):
        await bot.send_message(chat_id, chunk, reply_markup=reply_markup if i == 0 else None)


def parse_amount_expr(expr, chat_id, default_direct_unit=False):
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

    if suffix == "u" or (default_direct_unit and suffix == "" and rate_override is None and fee_override is None):
        unit_amount = sign * amt
        return {
            "raw_amount": None,
            "unit_amount": unit_amount,
            "rate_used": rate_used,
            "fee_used": fee_used,
        }

    if rate_used == 0:
        return None

    net_ratio = 1 - (fee_used / 100.0)
    unit_amount = sign * amt / rate_used * net_ratio
    raw_amount = sign * amt
    return {
        "raw_amount": raw_amount,
        "unit_amount": unit_amount,
        "rate_used": rate_used,
        "fee_used": fee_used,
    }


def split_target_prefix(text):
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


def format_tx_line(tx):
    tx_id, chat_id, user_id, username, display_name, target_name, kind, raw_amount, unit_amount, rate_used, fee_used, note, original_text, created_at, undone = tx
    tm = datetime.fromtimestamp(created_at).strftime("%H:%M:%S")

    if kind == "reserve":
        line = f"{tm} {fmt_num(unit_amount)}U"
        if note:
            line += f" {note}"
        if target_name:
            line += f" {target_name}"
        return line.strip()

    if raw_amount is not None:
        line = f"{tm} {fmt_num(raw_amount)} / {fmt_num(rate_used)} * ({1 - fee_used/100:.2f})={fmt_num(unit_amount)}U"
    else:
        line = f"{tm} {fmt_num(unit_amount)}U"

    extra = []
    if target_name:
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

    due = total_income_unit + total_reserve_unit
    paid = total_payout_unit
    pending = due - paid

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

    lines = [f"{title}"]

    lines.append(f"\n今日入款（{len(income_txs)}笔）")
    if income_txs:
        for tx in income_txs:
            lines.append(format_tx_line(tx))
    else:
        lines.append("暂无入款")

    lines.append(f"\n今日下发（{len(payout_txs)}笔）")
    if payout_txs:
        for tx in payout_txs:
            lines.append(format_tx_line(tx))
    else:
        lines.append("暂无下发")

    if reserve_txs:
        lines.append(f"\n账单寄存（{len(reserve_txs)}笔）")
        for tx in reserve_txs:
            lines.append(format_tx_line(tx))

    lines.append(f"\n分组统计（{len(get_groups())}组）")
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
        "记账机器人操作说明\n\n"
        "基础功能\n"
        "• 开始记账：开始\n"
        "• 停止记账：关闭记账\n"
        "• 打开发言：上课\n"
        "• 停止发言：下课\n\n"
        "配置\n"
        "• 设置汇率190\n"
        "• 设置费率7\n"
        "• 设置手续费20\n"
        "• 代付费率-5\n"
        "• 代付汇率8\n"
        "• 设置火币汇率190\n"
        "• 设置欧易汇率190\n"
        "• 设置实时汇率190\n\n"
        "操作员\n"
        "• @xxxx 添加操作员\n"
        "• @xxxx 删除操作员\n"
        "• 显示操作员\n"
        "• 回复某人：添加操作员 / 删除操作员\n\n"
        "记账\n"
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
        "查看\n"
        "• 账单 / /我\n"
        "• 我的账单（仅操作员）\n"
        "• 总账单\n"
        "• 上个月总账单\n"
        "• 撤销\n"
        "• 重置 / 清零 / 删除账单 / 结束账单\n\n"
        "广播\n"
        "• 群发广播\n\n"
        "试用\n"
        "• 申请试用\n"
        "• 激活码试用时长：10分钟\n"
    )


def main_menu_text():
    return (
        "记账机器人菜单\n\n"
        "请点击下方按钮，或直接在群里输入指令。"
    )


async def trial_expire_loop():
    while True:
        try:
            now_ts = int(time.time())
            expired_users = get_expired_access_users(now_ts)

            for user_id, username, expires_at in expired_users:
                try:
                    remove_access_user(user_id)
                    try:
                        await bot.send_message(
                            user_id,
                            "⏳ 您的试用权限已过期，请重新获取激活码。"
                        )
                    except Exception as e:
                        print("notify expired user failed:", e)
                except Exception as e:
                    print("remove expired access error:", e)

        except Exception as e:
            print("trial_expire_loop error:", e)

        await asyncio.sleep(30)


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
                        for chat_id, _ in get_groups():
                            if str(get_setting(chat_id, "no_daily_cut", "0")) == "1":
                                continue
                            clear_transactions(chat_id)
                            try:
                                await bot.send_message(chat_id, "🗓 日切已完成，当前账单已清空，开始新的一天。")
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

    global BOT_USERNAME
    try:
        me = await bot.get_me()
        BOT_USERNAME = me.username
    except Exception as e:
        print("get_me error:", e)

    webhook_url = f"{BASE_URL}/webhook" if BASE_URL else None
    print("webhook_url =", webhook_url)

    await asyncio.sleep(3)

    try:
        await bot.delete_webhook(drop_pending_updates=True)

        if webhook_url:
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

    task1 = asyncio.create_task(daily_cut_loop())
    task2 = asyncio.create_task(trial_expire_loop())

    try:
        yield
    finally:
        task1.cancel()
        task2.cancel()
        try:
            await task1
        except:
            pass
        try:
            await task2
        except:
            pass


app = FastAPI(lifespan=lifespan)


# ================= START / MENU =================
@dp.message(lambda m: m.text and is_cmd(m, "/start"))
async def start_cmd(m: types.Message):
    if not is_private(m):
        return

    text = get_setting(-1, "start_text") or (
        "📌 记账机器人菜单\n\n"
        "请点击下方按钮，或直接在群里输入指令。"
    )

    await m.answer(text, reply_markup=menu_kb())
    await m.answer("➕ 添加机器人到群：", reply_markup=start_inline_kb(m.from_user.id))


@dp.message(lambda m: m.text == "开始记账")
async def menu_begin(m: types.Message):
    if not is_private(m):
        return
    text = (
        "开始记账\n\n"
        "请把机器人添加进群，然后在群里输入：开始\n"
        "设置汇率：设置汇率190\n"
        "设置费率：设置费率7\n"
        "开始后即可输入 +10000、-10000、下发5000 等。"
    )
    await m.answer(text, reply_markup=menu_kb())


@dp.message(lambda m: m.text == "申请试用")
async def menu_trial(m: types.Message, state: FSMContext):
    if has_bot_access(m.from_user.id):
        return await m.reply("✅ 您已拥有使用权限。")

    await state.set_state(TrialFSM.waiting_code)
    await m.reply(
        "🔑 请输入激活码。\n\n"
        "输入正确后，您将获得10分钟的机器人使用权限。"
    )


@dp.message(TrialFSM.waiting_code)
async def receive_trial_code(m: types.Message, state: FSMContext):
    if not m.text:
        return

    code = m.text.strip()
    real_code = (get_trial_code() or "").strip()

    if not real_code:
        return await m.reply("❌ 当前未设置激活码，请联系管理员。")

    if code != real_code:
        return await m.reply("❌ 激活码错误，请重试。")

    expires_at = int(time.time()) + 10 * 60

    add_access_user(
        user_id=m.from_user.id,
        username=m.from_user.username or "",
        granted_by=None,
        expires_at=expires_at
    )

    await state.clear()
    await m.reply("✅ 激活成功，您已获得10分钟的机器人使用权限。")


@dp.message(lambda m: m.text == "自助续费")
async def menu_renew(m: types.Message):
    await m.answer("🔑 自助续费请联系管理员。")


@dp.message(lambda m: m.text == "实时U价")
async def menu_rate(m: types.Message):
    rate = get_setting(-1, "rate", "190")
    fee = get_setting(-1, "fee", "7")
    await m.answer(f"当前全局汇率：{rate}\n当前全局费率：{fee}%")


@dp.message(lambda m: m.text == "管理客服")
async def menu_support(m: types.Message):
    await m.answer("👨‍💼 管理客服：请填写你自己的客服联系方式。")


@dp.message(lambda m: m.text == "地址查询")
async def menu_address_query(m: types.Message):
    await m.reply(
        "🔍 请发送 TRON 地址进行查询。\n\n"
        "示例：\n"
        "TSPpLmYuFXLi6GU1W4uyG6NKGbdWPw886U"
    )


@dp.message(lambda m: m.text == "分组功能")
async def menu_group_func(m: types.Message):
    await m.answer(
        "分组功能\n\n"
        "• 本版本支持按群记账\n"
        "• 同时可设置操作员\n"
        "• 也可使用全局操作员\n"
        "• 账单统计会按当前群进行"
    )


@dp.message(lambda m: m.text == "使用说明")
async def menu_help(m: types.Message):
    await m.reply(help_text(), reply_markup=menu_kb())


# ================= ADMIN / ACCESS =================
@dp.message(lambda m: m.text and m.text.startswith("/settrialcode"))
async def set_trial_code_cmd(m: types.Message):
    if get_admin(m.from_user.id) != "super":
        return await m.reply("❌ 只有超级管理员可以设置激活码")

    parts = m.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await m.reply("用法：/settrialcode ABC123")

    code = parts[1].strip()
    set_trial_code(code)
    await m.reply(f"✅ 已设置激活码：{code}")


@dp.message(lambda m: m.text and m.text.startswith("/addaccess"))
async def add_access_cmd(m: types.Message):
    if get_admin(m.from_user.id) != "super":
        return await m.reply("❌ 只有超级管理员可以添加使用权限")

    target_id = None
    target_username = ""

    if m.reply_to_message and m.reply_to_message.from_user:
        u = m.reply_to_message.from_user
        target_id = u.id
        target_username = u.username or ""
    else:
        parts = m.text.split()
        if len(parts) >= 2 and parts[1].isdigit():
            target_id = int(parts[1])

    if not target_id:
        return await m.reply("用法：/addaccess 123456789 或回复某人消息后输入 /addaccess")

    add_access_user(target_id, target_username, granted_by=m.from_user.id, expires_at=None)
    await m.reply(f"✅ 已添加使用权限：{target_id}")


@dp.message(lambda m: m.text and m.text.startswith("/delaccess"))
async def del_access_cmd(m: types.Message):
    if get_admin(m.from_user.id) != "super":
        return await m.reply("❌ 只有超级管理员可以删除使用权限")

    target_id = None
    if m.reply_to_message and m.reply_to_message.from_user:
        target_id = m.reply_to_message.from_user.id
    else:
        parts = m.text.split()
        if len(parts) >= 2 and parts[1].isdigit():
            target_id = int(parts[1])

    if not target_id:
        return await m.reply("用法：/delaccess 123456789 或回复某人消息后输入 /delaccess")

    remove_access_user(target_id)
    await m.reply(f"✅ 已删除使用权限：{target_id}")


@dp.message(lambda m: m.text and m.text.startswith("/accesslist"))
async def access_list_cmd(m: types.Message):
    if get_admin(m.from_user.id) not in ("super", "admin"):
        return await m.reply("❌ 无权限")

    rows = get_access_users()
    if not rows:
        return await m.reply("暂无已授权用户")

    text = "已授权用户列表\n\n"
    for user_id, username, granted_by, granted_at, expires_at in rows:
        exp = "永久" if expires_at is None else datetime.fromtimestamp(expires_at).strftime("%Y-%m-%d %H:%M:%S")
        text += f"• {user_id} @{username or '-'} 到期：{exp}\n"
    await send_long_text(m.chat.id, text)


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

    text = "管理员列表\n\n"
    for uid, role in rows:
        text += f"• {uid} — {role}\n"
    await send_long_text(m.chat.id, text)


@dp.message(lambda m: m.text and is_cmd(m, "/myrole"))
async def myrole_cmd(m: types.Message):
    role = get_admin(m.from_user.id)
    await m.reply(f"你的权限：{role or '无'}")


# ================= OPERATOR =================
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
        out = "当前群操作员\n\n"
        if ops:
            for uid, uname, role in ops:
                out += f"• {uid or ''} @{uname or ''} {role}\n"
        else:
            out += "暂无群操作员\n"

        out += "\n全局操作员\n"
        if gops:
            for uid, uname, role in gops:
                out += f"• {uid or ''} @{uname or ''} {role}\n"
        else:
            out += "暂无全局操作员"
        return await send_long_text(m.chat.id, out)

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
    if get_admin(m.from_user.id) != "super":
        return await m.reply("❌ 只有超级管理员可以设置全局操作员")

    txt = m.text.strip()

    if "显示全局操作人" in txt:
        rows = get_global_operators()
        if not rows:
            return await m.reply("暂无全局操作员")
        out = "全局操作员\n\n"
        for uid, uname, role in rows:
            out += f"• {uid or ''} @{uname or ''} {role}\n"
        return await send_long_text(m.chat.id, out)

    if "删除所有人操作员" in txt or "取消全员" in txt:
        if not is_group_message(m):
            return await m.reply("请在群里使用此命令")
        clear_operators(m.chat.id)
        return await m.reply("✅ 已清空当前群操作员")

    if "全部记员" in txt and ("设置" in txt or "添加" in txt):
        if not is_group_message(m):
            return await m.reply("请在群里使用此命令")
        members = get_members(m.chat.id)
        for _, uid, uname, name, last_seen in members:
            try:
                add_operator(m.chat.id, user_id=uid, username=uname, role="operator")
            except:
                pass
        return await m.reply("✅ 已将当前群已记录成员设置为操作员")

    if "添加全局操作人" in txt:
        uid = None
        uname = None
        if m.reply_to_message and m.reply_to_message.from_user:
            u = m.reply_to_message.from_user
            uid = u.id
            uname = u.username or ""
        else:
            m2 = re.search(r"@([A-Za-z0-9_]+)", txt)
            if m2:
                uname = m2.group(1)

        if uid is None and not uname:
            return await m.reply("用法：@xxxx 添加全局操作人，或回复某人消息后输入")
        add_operator(-1, user_id=uid, username=uname, role="operator")
        return await m.reply("✅ 已添加全局操作人")

    if "删除全局操作人" in txt:
        uid = None
        uname = None
        if m.reply_to_message and m.reply_to_message.from_user:
            uid = m.reply_to_message.from_user.id
            uname = m.reply_to_message.from_user.username or ""
        else:
            m2 = re.search(r"@([A-Za-z0-9_]+)", txt)
            if m2:
                uname = m2.group(1)

        if uid is None and not uname:
            return await m.reply("用法：@xxxx 删除全局操作人，或回复某人消息后输入")
        remove_operator(-1, user_id=uid, username=uname)
        return await m.reply("✅ 已删除全局操作人")


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
    chat_id = m.chat.id if is_group_message(m) else -1

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


@dp.message(lambda m: m.text and m.text.startswith("设置按钮"))
async def set_buttons_cmd(m: types.Message):
    if not is_group_message(m):
        return await m.reply("请在群里设置按钮")
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    body = m.text[len("设置按钮"):].strip()
    data = parse_block_fields(body)

    updated = 0
    for i in range(1, 5):
        key = f"按钮{i}"
        val = data.get(key)
        if not val or "|" not in val:
            continue
        text, url = val.split("|", 1)
        text = text.strip()
        url = url.strip()
        if text and url:
            set_button_config(m.chat.id, i, text, url)
            updated += 1

    await m.reply(f"✅ 按钮已更新：{updated}个")


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

    text = (
        "当前配置\n\n"
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
    await m.reply(text)


# ================= START / STOP / PERMISSION =================
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
    chat_id = m.chat.id if is_group_message(m) else -1

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

    last = get_last_transaction(m.chat.id)
    if not last:
        return await m.reply("暂无可撤销记录")

    undo_transaction(last[0])
    await m.reply(f"✅ 已撤销上一笔账单：{last[0]}")


@dp.message(lambda m: m.text and m.text in ("账单", "/我", "我的账单"))
async def my_bill_cmd(m: types.Message):
    if not is_group_message(m):
        return
    ensure_group(m)

    target_user_id = m.from_user.id

    if m.text == "我的账单" and not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 仅操作员可查看“我的账单”")

    if m.reply_to_message and m.reply_to_message.from_user:
        target_user_id = m.reply_to_message.from_user.id

    txs = get_transactions(m.chat.id, user_id=target_user_id)
    stats = summarize_transactions(txs)

    text = (
        "我的账单\n\n"
        f"入款：{len([t for t in txs if t[6]=='income'])} 笔\n"
        f"下发：{len([t for t in txs if t[6]=='payout'])} 笔\n"
        f"总入款：{fmt_num(stats['total_income_unit'])}U\n"
        f"已下发：{fmt_num(stats['paid'])}U\n"
        f"未下发：{fmt_num(stats['pending'])}U"
    )
    await m.reply(text)


@dp.message(lambda m: m.text and m.text.endswith(" 账单"))
async def search_bill_cmd(m: types.Message):
    if not is_group_message(m):
        return
    ensure_group(m)

    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    kw = m.text[:-3].strip()
    if not kw or kw in ("账单", "我的账单"):
        return

    txs = get_transactions(m.chat.id, keyword=kw)
    if not txs:
        return await m.reply("暂无匹配账单")

    text = f"{kw} 的账单\n\n"
    for tx in txs[:20]:
        text += format_tx_line(tx) + "\n"
    await send_long_text(m.chat.id, text)


@dp.message(lambda m: m.text and m.text in ("总账单", "完整账单"))
async def full_report_cmd(m: types.Message):
    if not is_group_message(m):
        return
    ensure_group(m)
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    start_ts, end_ts = day_range()
    text = report_text(m.chat.id, start_ts, end_ts, title="今日总账")
    await send_long_text(m.chat.id, text, reply_markup=report_kb(m.chat.id))


@dp.message(lambda m: m.text and m.text in ("上个月总账单", "上月总账"))
async def last_month_report_cmd(m: types.Message):
    if not is_group_message(m):
        return
    ensure_group(m)
    if not is_admin_or_operator(m.chat.id, m.from_user):
        return await m.reply("❌ 无权限")

    start_ts, end_ts = month_range(1)
    text = report_text(m.chat.id, start_ts, end_ts, title="上月总账")
    await send_long_text(m.chat.id, text, reply_markup=report_kb(m.chat.id))


# ================= BROADCAST =================
@dp.message(lambda m: m.text == "群发广播")
async def menu_broadcast(m: types.Message, state: FSMContext):
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


@dp.message(BroadcastFSM.waiting_content)
async def broadcast_receive_content(m: types.Message, state: FSMContext):
    data = await state.get_data()
    scope = data.get("scope", "current")
    target_chat_id = data.get("target_chat_id", m.chat.id)

    await state.update_data(
        source_chat_id=m.chat.id,
        source_message_id=m.message_id,
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


# ================= AUTO LEDGER =================
@dp.message()
async def ledger_handler(m: types.Message):
    if should_ignore_message(m):
        return
    if not is_group_message(m):
        return
    if not m.text:
        return
    if m.text.startswith("/"):
        return

    ensure_group(m)

    if not get_enabled(m.chat.id):
        return

    txt = m.text.strip()

    if txt in ("+0", "-0", "0"):
        start_ts, end_ts = day_range()
        await send_long_text(
            m.chat.id,
            report_text(m.chat.id, start_ts, end_ts, title="今日账单"),
            reply_markup=report_kb(m.chat.id)
        )
        return

    locked_income_uid = get_chat_setting(m.chat.id, "locked_income_user_id", None)
    locked_payout_uid = get_chat_setting(m.chat.id, "locked_payout_user_id", None)

    # ---------- 寄存 ----------
    if txt.startswith("P+") or txt.startswith("P-"):
        if locked_income_uid and str(m.from_user.id) != str(locked_income_uid):
            return await m.reply("❌ 当前已锁定记账，非锁定人无法操作")

        parsed = parse_amount_expr(txt[1:], m.chat.id, default_direct_unit=True)
        if not parsed:
            return await m.reply("❌ 格式错误")

        unit_amount = parsed["unit_amount"]
        target = None
        if m.reply_to_message and m.reply_to_message.from_user:
            target = m.reply_to_message.from_user.full_name

        add_transaction(
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

        start_ts, end_ts = day_range()
        await send_long_text(
            m.chat.id,
            report_text(m.chat.id, start_ts, end_ts, title="今日账单"),
            reply_markup=report_kb(m.chat.id)
        )
        return

    # ---------- 下发 ----------
    if txt.startswith("下发"):
        if locked_payout_uid and str(m.from_user.id) != str(locked_payout_uid):
            return await m.reply("❌ 当前已锁定下发，非锁定人无法操作")

        body = txt[len("下发"):].strip()
        if not body:
            return await m.reply("格式：下发5000 / 下发-2000 / 下发1000R / 下发1000/7.8")

        has_conversion = ("R" in body) or ("r" in body) or ("/" in body) or ("*" in body)
        expr = body.replace("R", "").replace("r", "")
        parsed = parse_amount_expr(expr, m.chat.id, default_direct_unit=not has_conversion)
        if not parsed:
            return await m.reply("❌ 下发格式错误")

        target = None
        if m.reply_to_message and m.reply_to_message.from_user:
            target = m.reply_to_message.from_user.full_name

        add_transaction(
            chat_id=m.chat.id,
            user_id=m.from_user.id,
            username=m.from_user.username or "",
            display_name=m.from_user.full_name,
            target_name=target,
            kind="payout",
            raw_amount=parsed["raw_amount"],
            unit_amount=parsed["unit_amount"],
            rate_used=parsed["rate_used"],
            fee_used=parsed["fee_used"],
            note="下发",
            original_text=txt
        )

        start_ts, end_ts = day_range()
        await send_long_text(
            m.chat.id,
            report_text(m.chat.id, start_ts, end_ts, title="今日账单"),
            reply_markup=report_kb(m.chat.id)
        )
        return

    # ---------- 普通 + / - 记账 ----------
    target_name, body = split_target_prefix(txt)

    if not body or body[0] not in ("+", "-"):
        return

    if locked_income_uid and str(m.from_user.id) != str(locked_income_uid):
        return await m.reply("❌ 当前已锁定记账，非锁定人无法操作")

    note = ""
    if " " in body:
        first_part, note = body.split(" ", 1)
        amount_expr = first_part.strip()
        note = note.strip()
    else:
        amount_expr = body.strip()

    parsed = parse_amount_expr(amount_expr, m.chat.id, default_direct_unit=False)
    if not parsed:
        return await m.reply("❌ 记账格式错误")

    kind = "income" if amount_expr.startswith("+") else "payout"

    if not target_name:
        if m.reply_to_message and m.reply_to_message.from_user:
            target_name = m.reply_to_message.from_user.full_name
        else:
            target_name = ""

    add_transaction(
        chat_id=m.chat.id,
        user_id=m.from_user.id,
        username=m.from_user.username or "",
        display_name=m.from_user.full_name,
        target_name=target_name,
        kind=kind,
        raw_amount=parsed["raw_amount"],
        unit_amount=parsed["unit_amount"],
        rate_used=parsed["rate_used"],
        fee_used=parsed["fee_used"],
        note=note,
        original_text=txt
    )

    start_ts, end_ts = day_range()
    await send_long_text(
        m.chat.id,
        report_text(m.chat.id, start_ts, end_ts, title="今日账单"),
        reply_markup=report_kb(m.chat.id)
    )
    return


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
    await c.message.reply(report_text(c.message.chat.id, start_ts, end_ts, title="今日账单"), reply_markup=report_kb(c.message.chat.id))
    await c.answer()


# ================= WALLET LOGS =================
@dp.message(lambda m: m.text == "交易记录")
async def wallet_logs_menu(m: types.Message):
    rows = get_wallet_checks_page(limit=10, offset=0)
    if not rows:
        return await m.reply("暂无历史记录。")

    total = count_wallet_checks()
    buttons = []
    text_lines = [
        "📄 最近交易",
        "📍 当前页码：第 1 页",
        ""
    ]

    for row in rows:
        _id, chat_id, user_id, username, full_name, address, trx_balance, usdt_balance, tx_count, created_at = row
        sender = full_name or username or str(user_id)
        tm = fmt_wallet_ts(created_at)

        text_lines.append(
            f"🕒 {tm}\n"
            f"👤 {sender}\n"
            f"📌 {address}\n"
            f"💰 TRX: {fmt_num(trx_balance)} | USDT: {fmt_num(usdt_balance)}\n"
            f"📊 交易次数: {tx_count if tx_count is not None else 'N/A'}\n"
            f"{'—' * 24}"
        )

        buttons.append([
            InlineKeyboardButton(
                text=f"🔗 {address[:8]}...",
                url=f"https://tronscan.org/#/address/{address}"
            )
        ])

    if total > 10:
        buttons.append([
            InlineKeyboardButton(text="下一页 ➡️", callback_data="wallet:recent:1")
        ])

    await m.reply(
        "\n".join(text_lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        disable_web_page_preview=True
    )


@dp.callback_query(lambda c: c.data and (c.data.startswith("wallet:logs:") or c.data.startswith("wallet:recent:")))
async def wallet_logs_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return

    try:
        page = int(c.data.split(":")[-1])
    except:
        page = 0

    limit = 10
    offset = page * limit

    rows = get_wallet_checks_page(limit=limit, offset=offset)
    if not rows:
        return await c.message.edit_text("暂无历史记录。")

    total = count_wallet_checks()
    has_prev = page > 0
    has_next = offset + limit < total

    text_lines = [
        "📄 最近交易",
        f"📍 当前页码：第 {page + 1} 页",
        ""
    ]

    buttons = []

    for row in rows:
        _id, chat_id, user_id, username, full_name, address, trx_balance, usdt_balance, tx_count, created_at = row
        sender = full_name or username or str(user_id)
        tm = fmt_wallet_ts(created_at)

        text_lines.append(
            f"🕒 {tm}\n"
            f"👤 {sender}\n"
            f"📌 {address}\n"
            f"💰 TRX: {fmt_num(trx_balance)} | USDT: {fmt_num(usdt_balance)}\n"
            f"📊 交易次数: {tx_count if tx_count is not None else 'N/A'}\n"
            f"{'—' * 24}"
        )

        buttons.append([
            InlineKeyboardButton(
                text=f"🔗 {address[:8]}...",
                url=f"https://tronscan.org/#/address/{address}"
            )
        ])

    nav = []
    if has_prev:
        nav.append(InlineKeyboardButton(
            text="⬅️ 上一页",
            callback_data=f"wallet:recent:{page - 1}"
        ))
    if has_next:
        nav.append(InlineKeyboardButton(
            text="下一页 ➡️",
            callback_data=f"wallet:recent:{page + 1}"
        ))

    if nav:
        buttons.append(nav)

    await c.message.edit_text(
        "\n".join(text_lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        disable_web_page_preview=True
    )
    await c.answer()


# ================= USER JOIN =================
@dp.message(lambda m: m.new_chat_members)
async def new_members(m: types.Message):
    ensure_group(m)
    text = f"欢迎 {', '.join([u.full_name for u in m.new_chat_members])} 来到本群。\n记账机器人已就绪。"
    try:
        await m.reply(text)
    except:
        pass


# ================= BOT JOIN =================
@dp.my_chat_member()
async def on_bot_member_update(e: types.ChatMemberUpdated):
    try:
        if e.new_chat_member.status in ("member", "administrator") and e.old_chat_member.status == "left":
            save_group(e.chat.id, e.chat.title or "Unnamed group")
            await bot.send_message(e.chat.id, "✅ 记账机器人已加入本群。")
    except Exception as ex:
        print("on_bot_member_update error:", ex)


# ================= WEBHOOK / HEALTH =================
@app.post("/webhook")
async def webhook(req: Request):
    try:
        data = await req.json()
        update = types.Update.model_validate(data)
        await dp.feed_update(bot, update)
        return {"ok": True}
    except Exception as e:
        print("webhook error:", e)
        return {"ok": False}


@app.get("/healthz")
@app.head("/healthz")
def healthz():
    return {"ok": True}


@app.get("/")
@app.head("/")
def home():
    return {"status": "running"}


# ================= RUN =================
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT)
