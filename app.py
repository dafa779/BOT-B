
import os
import re
import time
import asyncio
import requests
import aiohttp
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
from urllib.parse import quote
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

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
    CopyTextButton,
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

    # rental/order helpers (must exist in db.py)
    create_rental_order,
    get_rental_order,
    get_pending_rental_orders,
    get_rental_orders_by_status,
    mark_rental_order_paid,
    mark_rental_order_rejected,
    get_access_user_by_id,
    has_expiry_notice,
    add_expiry_notice,
)

# ================= ENV =================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "8080"))
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID", "0") or 0)
SUPER_ADMIN_ID = int(os.getenv("SUPER_ADMIN_ID", "0") or 0)
WEB_TOKEN = os.getenv("WEB_TOKEN", "abc123")
BASE_URL = (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("BASE_URL") or "").rstrip("/")

TRONGRID_API_KEY = os.getenv("TRONGRID_API_KEY", "")
TRONGRID_API_URL = "https://api.trongrid.io"
PAYMENT_ADDRESS = os.getenv("PAYMENT_ADDRESS", "TSPpLmYuFXLi6GU1W4uyG6NKGbdWPw886U")
PAYMENT_SUPPORT = os.getenv("PAYMENT_SUPPORT", "/ZZB339")

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

# ================= CONSTANTS =================
BEIJING_TZ = ZoneInfo("Asia/Shanghai")
USDT_TRC20_CONTRACT = "TXLAQ63Xg1NAzckPwKHvzw7CSEmLMEqcdj"

RATE_CACHE = {"value": None, "ts": 0}
RATE_CACHE_TTL = 30
USDT_DAILY_UPDATE_KEY = "usdt_daily_update_date"

TRON_ADDR_RE = re.compile(r"\bT[1-9A-HJ-NP-Za-km-z]{33}\b")

# ================= STATES =================
class BroadcastFSM(StatesGroup):
    waiting_content = State()
    waiting_confirm = State()

class TrialFSM(StatesGroup):
    waiting_code = State()
    waiting_create_code = State()

class AdminFSM(StatesGroup):
    waiting_add_admin = State()
    waiting_del_admin = State()
    waiting_trial_code = State()

class AddressQueryFSM(StatesGroup):
    waiting_address = State()

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
    return (not m or not m.from_user or m.from_user.is_bot or not m.text)

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

def get_user_role(user_id):
    if BOT_OWNER_ID and int(user_id) == int(BOT_OWNER_ID):
        return "owner"
    if SUPER_ADMIN_ID and int(user_id) == int(SUPER_ADMIN_ID):
        return "super"
    role = get_admin(user_id)
    if role == "admin":
        return "admin"
    if role == "super":
        return "super"
    return None

def can_use_manage_panel(user_id):
    return get_user_role(user_id) in ("owner", "super", "admin")

def can_use_bot_ops(user_id):
    return get_user_role(user_id) in ("owner", "super", "admin")

def can_manage_codes(user_id):
    return get_user_role(user_id) in ("owner", "super")

def can_manage_admins(user_id):
    return get_user_role(user_id) == "owner"

def deny_text():
    return "❌ 无权限"

def has_bot_access(user_id):
    return get_user_role(user_id) in ("owner", "super", "admin") or has_access_user(user_id)

def is_tron_address(addr: str):
    if not addr:
        return False
    addr = addr.strip()
    return bool(re.fullmatch(r"T[1-9A-HJ-NP-Za-km-z]{33}", addr))

def extract_tron_address(text: str):
    if not text:
        return None
    m = TRON_ADDR_RE.search(text.strip())
    return m.group(0) if m else None

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

def trial_claim_key(user_id: int):
    return f"trial_claimed:{user_id}"

def has_claimed_free_trial(user_id: int):
    return str(get_setting(-1, trial_claim_key(user_id), "0")) == "1"

def mark_claimed_free_trial(user_id: int):
    set_setting(-1, trial_claim_key(user_id), "1")

# ================= UI =================
def menu_kb(user_id=None):
    keyboard = [
        [KeyboardButton(text="🔥 开始记账")],
        [
            KeyboardButton(text="💎 申请试用"),
            KeyboardButton(text="📝 使用说明"),
        ],
        [
            KeyboardButton(text="📈 实时U价"),
            KeyboardButton(text="🔍 地址查询"),
        ],
        [
            KeyboardButton(text="🔑 自助续费"),
            KeyboardButton(text="📋 复制命令"),
        ],
        [
            KeyboardButton(text="📣 群发广播"),
            KeyboardButton(text="📜 交易历史"),
        ],
    ]

    if user_id is not None and can_use_manage_panel(user_id):
        keyboard.append([KeyboardButton(text="🛠 管理面板")])

    keyboard.append([KeyboardButton(text="👥 分组功能")])

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        one_time_keyboard=False
    )

def start_inline_kb(user_id=None):
    if BOT_USERNAME:
        add_url = f"https://t.me/{BOT_USERNAME}?startgroup=add"
    else:
        add_url = "https://t.me/"

    buttons = [
        [InlineKeyboardButton(text="➕ 添加机器人到群", url=add_url)],
        [InlineKeyboardButton(text="📝 使用说明", callback_data="menu:help")],
        [InlineKeyboardButton(text="📋 复制命令", callback_data="menu:copy")],
    ]

    if user_id is not None and can_manage_codes(user_id):
        buttons.append([InlineKeyboardButton(text="🔑 创建激活码", callback_data="manage:create_code")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)

def copy_cmd_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 复制：开始", copy_text=CopyTextButton(text="开始")),
            InlineKeyboardButton(text="📋 复制：总账单", copy_text=CopyTextButton(text="总账单")),
        ],
        [
            InlineKeyboardButton(text="📋 复制：设置汇率190", copy_text=CopyTextButton(text="设置汇率190")),
            InlineKeyboardButton(text="📋 复制：设置费率7", copy_text=CopyTextButton(text="设置费率7")),
        ],
        [
            InlineKeyboardButton(text="📋 复制：地址查询", copy_text=CopyTextButton(text="地址查询")),
            InlineKeyboardButton(text="📋 复制：撤销", copy_text=CopyTextButton(text="撤销")),
        ],
        [
            InlineKeyboardButton(text="📋 复制：群发广播", copy_text=CopyTextButton(text="群发广播")),
            InlineKeyboardButton(text="📋 复制：使用说明", copy_text=CopyTextButton(text="使用说明")),
        ],
    ])

def admin_copy_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ 添加ADMIN", copy_text=CopyTextButton(text="/addadmin 123456789")),
            InlineKeyboardButton(text="➖ 删除ADMIN", copy_text=CopyTextButton(text="/deladmin 123456789")),
            InlineKeyboardButton(text="📋 ADMIN列表", copy_text=CopyTextButton(text="/admins")),
        ],
        [
            InlineKeyboardButton(text="🔑 创建续费码", copy_text=CopyTextButton(text="/settrialcode ABC123")),
            InlineKeyboardButton(text="🗑 收回续费码", copy_text=CopyTextButton(text="/revoketrialcode")),
        ],
    ])

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

def begin_help_text():
    return (
        "🔥 <b>开始记账</b>\n\n"
        "请先将机器人添加到群聊，并授予必要权限。\n\n"
        "<b>常用命令</b>\n"
        "• <code>开始</code>\n"
        "• <code>关闭记账</code>\n"
        "• <code>设置汇率190</code>\n"
        "• <code>设置费率7</code>\n"
        "• <code>+1000</code>\n"
        "• <code>-1000</code>\n"
        "• <code>下发5000</code>\n"
        "• <code>P+2000</code>\n"
        "• <code>总账单</code>\n"
        "• <code>撤销</code>\n"
    )

def begin_copy_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 开始", callback_data="copy:开始"),
            InlineKeyboardButton(text="📋 关闭记账", callback_data="copy:关闭记账"),
        ],
        [
            InlineKeyboardButton(text="📋 设置汇率190", callback_data="copy:设置汇率190"),
            InlineKeyboardButton(text="📋 设置费率7", callback_data="copy:设置费率7"),
        ],
        [
            InlineKeyboardButton(text="📋 +1000", callback_data="copy:+1000"),
            InlineKeyboardButton(text="📋 -1000", callback_data="copy:-1000"),
        ],
        [
            InlineKeyboardButton(text="📋 下发5000", callback_data="copy:下发5000"),
            InlineKeyboardButton(text="📋 P+2000", callback_data="copy:P+2000"),
        ],
        [
            InlineKeyboardButton(text="📋 总账单", callback_data="copy:总账单"),
            InlineKeyboardButton(text="📋 撤销", callback_data="copy:撤销"),
        ],
    ])

def main_menu_text():
    return (
        "📌 记账机器人菜单\n\n"
        "请选择下方功能，或直接在群里输入指令。"
    )

def quick_key_text():
    return (
        "🗝 <b>记忆 Key</b>\n\n"
        "<b>开关</b>\n"
        "• 开始：<code>开始</code>\n"
        "• 关闭：<code>关闭记账</code>\n"
        "• 发言：<code>上课</code> / <code>下课</code>\n\n"
        "<b>参数</b>\n"
        "• 汇率：<code>设置汇率190</code>\n"
        "• 费率：<code>设置费率7</code>\n\n"
        "<b>记账</b>\n"
        "• 入账：<code>+1000</code>\n"
        "• 出账：<code>-1000</code>\n"
        "• 下发：<code>下发5000</code>\n"
        "• 寄存：<code>P+2000</code>\n"
        "• 备注：<code>+1000 备注</code>\n\n"
        "<b>查看</b>\n"
        "• 今日总账：<code>总账单</code>\n"
        "• 个人账单：<code>账单</code>\n"
        "• 我的账单：<code>/我</code>\n"
        "• 撤销：<code>撤销</code>\n"
    )

def quick_key_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="开始", callback_data="copy:开始"),
            InlineKeyboardButton(text="关闭记账", callback_data="copy:关闭记账"),
        ],
        [
            InlineKeyboardButton(text="上课", callback_data="copy:上课"),
            InlineKeyboardButton(text="下课", callback_data="copy:下课"),
        ],
        [
            InlineKeyboardButton(text="设置汇率190", callback_data="copy:设置汇率190"),
            InlineKeyboardButton(text="设置费率7", callback_data="copy:设置费率7"),
        ],
        [
            InlineKeyboardButton(text="+1000", callback_data="copy:+1000"),
            InlineKeyboardButton(text="-1000", callback_data="copy:-1000"),
        ],
        [
            InlineKeyboardButton(text="下发5000", callback_data="copy:下发5000"),
            InlineKeyboardButton(text="P+2000", callback_data="copy:P+2000"),
        ],
        [
            InlineKeyboardButton(text="总账单", callback_data="copy:总账单"),
            InlineKeyboardButton(text="撤销", callback_data="copy:撤销"),
        ],
        [
            InlineKeyboardButton(text="📝 使用说明", callback_data="menu:help"),
        ]
    ])

def help_text():
    return (
        "📚 记账机器人使用说明\n\n"
        "【基础功能】\n"
        "• 开始记账：开始 / 🔥 开始记账\n"
        "• 停止记账：关闭记账 / 停止记账\n"
        "• 打开发言：上课\n"
        "• 停止发言：下课\n\n"
        "【参数设置】\n"
        "• 设置汇率：设置汇率190\n"
        "• 设置费率：设置费率7\n"
        "• 单笔手续费：单笔手续费20\n"
        "• 代付费率：代付费率-5\n"
        "• 代付汇率：代付汇率8\n"
        "• 实时汇率：设置实时汇率190\n\n"
        "【记账指令】\n"
        "• +1000 / -1000\n"
        "• +1000/7.8 / -1000/7.8\n"
        "• +7777u / -7777u\n"
        "• 下发5000 / 下发-2000 / 下发1000R\n"
        "• P+2000 / P-1000\n"
        "• +1000 备注\n\n"
        "【查看功能】\n"
        "• 总账单\n"
        "• 账单\n"
        "• /我\n"
        "• 撤销\n"
        "• 上个月总账单\n\n"
        "【试用与续费】\n"
        "• 申请试用：首次可领取 24 小时免费权限\n"
        "• 到期后：输入管理员发放的续费码继续使用\n"
        "• 续费码可由主控管理员发放与收回\n\n"
        "【快捷复制】\n"
        "• 菜单里的 📋 复制命令 可一键复制常用指令\n\n"
        "【可定制机器人】\n"
        "• 电脑管理机器人\n"
        "• 机器人报表机器人\n"
        "• 客服机器人\n"
        "• 风控提醒机器人\n"
        "• 广播管理机器人\n"
        "• 自动统计机器人\n\n"
        "如需新增功能、修改界面、定制按钮，请联系管理员。"
    )

def address_query_text():
    return (
        "🔍 <b>地址查询</b>\n\n"
        "请直接发送 TRON 地址进行查询。\n\n"
        "<b>示例：</b>\n"
        "<code>TSPpLmYuFXLi6GU1W4uyG6NKGbdWPw886U</code>"
    )

def address_result_kb(address, page=1):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📜 群组交易记录", callback_data=f"addr:tx:{address}:{page}"),
            InlineKeyboardButton(text="🔄 重新查询", callback_data="addr:again")
        ],
        [InlineKeyboardButton(text="⬅️ 返回菜单", callback_data="addr:back")]
    ])

def tx_history_kb(address, page=1):
    buttons = []
    if page > 1:
        buttons.append(InlineKeyboardButton(text="⬅️ 上一页", callback_data=f"addr:tx:{address}:{page-1}"))
    buttons.append(InlineKeyboardButton(text=f"📄 第 {page} 页", callback_data="noop"))
    buttons.append(InlineKeyboardButton(text="下一页 ➡️", callback_data=f"addr:tx:{address}:{page+1}"))
    return InlineKeyboardMarkup(inline_keyboard=[buttons])

def rent_main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 Bot quản trị nhóm", callback_data="rent:group_admin")],
        [InlineKeyboardButton(text="💻 Bot máy tính", callback_data="rent:computer")],
        [InlineKeyboardButton(text="🌐 Bot dịch thuật", callback_data="rent:translator")],
        [InlineKeyboardButton(text="⬅️ Quay lại", callback_data="rent:back")],
    ])

RENT_CATEGORIES = {
    "group_admin": {"title": "🤖 Bot quản trị nhóm"},
    "computer": {"title": "💻 Bot máy tính"},
    "translator": {"title": "🌐 Bot dịch thuật"},
}

RENT_PLANS = {
    "1m": {"label": "一个月", "amount": 100},
    "3m": {"label": "三个月", "amount": 230},
    "6m": {"label": "六个月", "amount": 400},
    "1y": {"label": "一年", "amount": 700},
}

def rent_plan_kb(category_key):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="一个月 (100U)", callback_data=f"rent:plan:{category_key}:1m")],
        [InlineKeyboardButton(text="三个月 (230U)", callback_data=f"rent:plan:{category_key}:3m")],
        [InlineKeyboardButton(text="六个月 (400U)", callback_data=f"rent:plan:{category_key}:6m")],
        [InlineKeyboardButton(text="一年 (700U)", callback_data=f"rent:plan:{category_key}:1y")],
        [InlineKeyboardButton(text="⬅️ 返回套餐", callback_data="rent:main")],
    ])

def rent_payment_text(category_key, plan_key, order_code):
    cat = RENT_CATEGORIES.get(category_key, {})
    plan = RENT_PLANS.get(plan_key, {})
    title = cat.get("title", "套餐")
    plan_label = plan.get("label", "")
    amount = plan.get("amount", 0)

    return (
        f"✅ <b>{title}</b>\n"
        f"📦 套餐：<b>{plan_label}</b>\n"
        f"🧾 订单号：<code>{order_code}</code>\n\n"
        f"🌿 <b>收款地址：TRC20-USDT</b>\n"
        f"┆\n"
        f"├ 💰订单金额：<b>{amount} U</b>\n"
        f"┆\n"
        f"└➤ <code>{PAYMENT_ADDRESS}</code>\n\n"
        f"🦉 点击(地址和金额)自动复制\n"
        f"- - - - - - - - - - - - - - - - - - - - -\n"
        f"注意：请务必按指定金额 <b>{amount} U</b> 转账\n"
        f"付款后10秒钟自动开通成功\n"
        f"- - - - - - - - - - - - - - - - - - - - -\n"
        f"🗣️ 在线24小时客服 <code>{PAYMENT_SUPPORT}</code>"
    )

def rent_payment_kb(amount):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 复制地址", callback_data=f"copy:{PAYMENT_ADDRESS}"),
            InlineKeyboardButton(text=f"📋 复制金额 {amount}U", callback_data=f"copy:{amount}"),
        ],
        [
            InlineKeyboardButton(text="⬅️ 返回套餐", callback_data="rent:main"),
            InlineKeyboardButton(text="🔄 重新选择", callback_data="rent:back"),
        ],
    ])

def manage_panel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ 添加管理员", callback_data="manage:add_admin"),
            InlineKeyboardButton(text="➖ 删除管理员", callback_data="manage:del_admin"),
        ],
        [
            InlineKeyboardButton(text="📋 管理员列表", callback_data="manage:list_admin"),
        ],
        [
            InlineKeyboardButton(text="🔑 创建续费码", callback_data="manage:create_code"),
            InlineKeyboardButton(text="🗑 回收续费码", callback_data="manage:revoke_code"),
        ],
    ])

def history_groups_kb():
    groups = get_groups()
    rows = []
    for chat_id, title in groups:
        today = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")
        url = f"{BASE_URL}/group/{chat_id}?date={today}&token={WEB_TOKEN}"
        rows.append([InlineKeyboardButton(text=f"📂 {title}", url=url)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def order_history_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 全部订单", callback_data="order:history:all")],
        [
            InlineKeyboardButton(text="⏳ 待支付", callback_data="order:history:pending"),
            InlineKeyboardButton(text="✅ 已支付", callback_data="order:history:paid"),
        ],
        [InlineKeyboardButton(text="❌ 已拒绝", callback_data="order:history:rejected")],
    ])

# ================= TEXT HELPERS =================
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

# ================= TRON API =================
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

            if sym == "USDT" or contract == USDT_TRC20_CONTRACT:
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

        if TRONGRID_API_KEY:
            headers["TRON-PRO-API-KEY"] = TRONGRID_API_KEY

        sources = [
            f"https://api.trongrid.io/v1/accounts/{address}",
            f"https://apilist.tronscanapi.com/api/account?address={address}",
        ]

        for url in sources:
            try:
                r = requests.get(url, timeout=15, headers=headers)
                if not r.ok:
                    continue

                payload = r.json()
                acc = _pick_account(payload)
                if acc:
                    source_name = "trongrid" if "trongrid" in url else "tronscan"
                    return {"source": source_name, "account": acc}
            except Exception as e:
                print("wallet api error:", url, e)

        return None

    result = await asyncio.to_thread(_fetch)
    if not result:
        return None

    acc = result["account"]

    trx_balance = None
    try:
        if acc.get("balance") is not None:
            trx_balance = float(acc.get("balance")) / 1_000_000
    except Exception as e:
        print("trx_balance parse error:", e)
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

async def get_tron_transactions(address, page=1, page_size=10):
    offset = (page - 1) * page_size
    tx_data = await trongrid_get(
        f"/v1/accounts/{address}/transactions",
        params={"limit": page_size, "only_confirmed": "true", "order_by": "block_timestamp,desc", "offset": offset}
    )
    return tx_data.get("data", []) if tx_data else []

def format_tron_tx_row(tx):
    try:
        ts = tx.get("block_timestamp")
        dt = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S") if ts else "-"
        txid = tx.get("txID", "-")
        contract = tx.get("raw_data", {}).get("contract", [])
        tx_type = "-"
        if contract:
            tx_type = contract[0].get("type", "-")
        return f"• {dt} | {tx_type}\n  <code>{txid}</code>"
    except:
        return "• 无法解析交易"

def format_address_info_text(address, info):
    if not info:
        return (
            f"🔎 查询地址：<code>{address}</code>\n\n"
            "⚠️ 无法获取链上数据，请稍后重试。"
        )

    trx_balance = info.get("trx_balance", 0)
    usdt_balance = info.get("usdt_balance", 0)
    tx_count = info.get("tx_count", 0)
    first_tx = info.get("create_time") or "-"
    last_active = info.get("latest_time") or "-"
    is_multisig = False
    sig_status = "多签地址" if is_multisig else "未多签地址"

    return (
        f"🔎 查询地址：<code>{address}</code>\n\n"
        f"💡 交易次数：{tx_count}\n"
        f"⏰ 首次交易：{fmt_ts(first_tx)}\n"
        f"🌟 最后活跃：{fmt_ts(last_active)}\n"
        f"🔰 签名状态：{sig_status}\n\n"
        f"🔋 能量：剩余：0 / 0\n"
        f"🌈 带宽：剩余：600 / 600\n\n"
        f"💰 USDT余额：{fmt_num(usdt_balance)} USDT\n"
        f"💰 TRX 余额：{fmt_num(trx_balance)} TRX"
    )

# ================= RENT / ORDER HELPERS =================
def plan_duration_seconds(plan_key):
    if plan_key == "1m":
        return 30 * 24 * 60 * 60
    if plan_key == "3m":
        return 90 * 24 * 60 * 60
    if plan_key == "6m":
        return 180 * 24 * 60 * 60
    if plan_key == "1y":
        return 365 * 24 * 60 * 60
    return 30 * 24 * 60 * 60

def calc_renew_expire_at(user_id, plan_key):
    now_ts = int(time.time())
    duration = plan_duration_seconds(plan_key)
    access_row = get_access_user_by_id(user_id)
    current_exp = None
    if access_row and len(access_row) >= 5:
        current_exp = access_row[4]
    base_ts = now_ts
    if current_exp and int(current_exp) > now_ts:
        base_ts = int(current_exp)
    return base_ts + duration

async def activate_rental_order(order_code, granted_by=None):
    row = get_rental_order(order_code)
    if not row:
        return None, "订单不存在"

    (
        order_code, user_id, username, full_name, category_key, category_title,
        plan_key, plan_label, amount, status, created_at, paid_at, expires_at, note
    ) = row

    if status == "paid":
        return row, "订单已支付"

    new_expires_at = calc_renew_expire_at(user_id, plan_key)
    mark_rental_order_paid(order_code, expires_at=new_expires_at)

    add_access_user(
        user_id=user_id,
        username=username or "",
        granted_by=granted_by,
        expires_at=new_expires_at
    )

    return row, None

# ================= USDT RATE =================
async def fetch_usdt_rates():
    urls = [
        "https://open.er-api.com/v6/latest/USD",
        "https://api.exchangerate.host/latest?base=USD&symbols=CNY,VND",
    ]

    for url in urls:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=8) as resp:
                    data = await resp.json()

                    if data.get("result") == "success" and "rates" in data:
                        rates = data["rates"]
                        return {
                            "usd_cny": float(rates.get("CNY")) if rates.get("CNY") else None,
                            "usd_vnd": float(rates.get("VND")) if rates.get("VND") else None,
                        }

                    rates = data.get("rates", {})
                    return {
                        "usd_cny": float(rates.get("CNY")) if rates.get("CNY") else None,
                        "usd_vnd": float(rates.get("VND")) if rates.get("VND") else None,
                    }

        except Exception as e:
            print("fetch_usdt_rates error:", e)

    return None

def format_usdt_rate_text(rates):
    now_str = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    cny = rates.get("usd_cny") if rates else None
    vnd = rates.get("usd_vnd") if rates else None

    lines = ["📈 <b>实时U价</b>", ""]

    if cny:
        lines.append(f"🇨🇳 市场价：<code>{cny:.4f}</code> CNY / USDT")
        lines.append(f"• 1 CNY ≈ <code>{1/cny:.4f}</code> USDT")
    else:
        lines.append("🇨🇳 市场价：<i>获取失败</i>")

    if vnd:
        lines.append(f"🇻🇳 市场价：<code>{vnd:,.0f}</code> VND / USDT")
        lines.append(f"• 1 VND ≈ <code>{1/vnd:.8f}</code> USDT")
    else:
        lines.append("🇻🇳 市场价：<i>获取失败</i>")

    lines += ["", f"🕒 更新时间：<code>{now_str}</code>"]
    return "\n".join(lines)

def rate_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 刷新价格", callback_data="rate:refresh")],
        [InlineKeyboardButton(text="📝 使用说明", callback_data="menu:help")],
    ])

async def get_usdt_rates_cached(force=False):
    now = time.time()
    if not force and RATE_CACHE["value"] and (now - RATE_CACHE["ts"] < RATE_CACHE_TTL):
        return RATE_CACHE["value"]
    rates = await fetch_usdt_rates()
    if rates:
        RATE_CACHE["value"] = rates
        RATE_CACHE["ts"] = now
        return rates
    return RATE_CACHE["value"]

async def daily_usdt_update_loop():
    while True:
        try:
            now = datetime.now(BEIJING_TZ)
            today_key = now.strftime("%Y-%m-%d")
            target_time = now.replace(hour=8, minute=0, second=0, microsecond=0)
            last_update_date = get_setting(-1, USDT_DAILY_UPDATE_KEY, "")

            if now >= target_time and last_update_date != today_key:
                rates = await fetch_usdt_rates()
                if rates:
                    RATE_CACHE["value"] = rates
                    RATE_CACHE["ts"] = time.time()
                    set_setting(-1, USDT_DAILY_UPDATE_KEY, today_key)
                    print(f"[USDT] Updated at {now.strftime('%Y-%m-%d %H:%M:%S')} Beijing time")

            if now < target_time:
                sleep_seconds = (target_time - now).total_seconds()
                await asyncio.sleep(min(sleep_seconds, 60))
            else:
                await asyncio.sleep(60)

        except Exception as e:
            print("daily_usdt_update_loop error:", e)
            await asyncio.sleep(60)

# ================= RENT MENU =================
@dp.message(lambda m: m.text in ("🔑 自助续费", "自助续费", "续费/租用"))
async def menu_rent(m: types.Message):
    await m.answer("🔑 <b>请选择要租用的机器人类型</b>", reply_markup=rent_main_kb(), parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "rent:main")
async def rent_main_cb(c: types.CallbackQuery):
    if not c.message:
        return
    await c.message.answer("🔑 <b>请选择要租用的机器人类型</b>", reply_markup=rent_main_kb(), parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data == "rent:back")
async def rent_back_cb(c: types.CallbackQuery):
    if not c.message:
        return
    await c.message.answer("🔑 <b>请选择要租用的机器人类型</b>", reply_markup=rent_main_kb(), parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data in ("rent:group_admin", "rent:computer", "rent:translator"))
async def rent_category_cb(c: types.CallbackQuery):
    if not c.message:
        return
    category_key = c.data.split(":")[1]
    title = RENT_CATEGORIES.get(category_key, {}).get("title", "套餐")
    await c.message.answer(f"📦 <b>{title}</b>\n\n请选择租用时长：", reply_markup=rent_plan_kb(category_key), parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("rent:plan:"))
async def rent_plan_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return

    _, _, category_key, plan_key = c.data.split(":", 3)
    cat = RENT_CATEGORIES.get(category_key)
    plan = RENT_PLANS.get(plan_key)

    if not cat or not plan:
        return await c.answer("套餐不存在", show_alert=True)

    category_title = cat["title"]
    plan_label = plan["label"]
    amount = plan["amount"]

    order_code = create_rental_order(
        user_id=c.from_user.id,
        username=c.from_user.username or "",
        full_name=c.from_user.full_name or "",
        category_key=category_key,
        category_title=category_title,
        plan_key=plan_key,
        plan_label=plan_label,
        amount=amount,
        note="rent_order"
    )

    text = rent_payment_text(category_key, plan_key, order_code)
    await c.message.answer(text, reply_markup=rent_payment_kb(amount), parse_mode="HTML")
    await c.answer("✅ 已生成订单")

# ================= ORDER MANAGEMENT =================
@dp.callback_query(lambda c: c.data and c.data.startswith("order:view:"))
async def view_order_cb(c: types.CallbackQuery):
    if not c.message:
        return

    order_code = c.data.split(":", 2)[2]
    row = get_rental_order(order_code)
    if not row:
        return await c.answer("订单不存在", show_alert=True)

    (
        order_code, user_id, username, full_name, category_key, category_title,
        plan_key, plan_label, amount, status, created_at, paid_at, expires_at, note
    ) = row

    created_str = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
    paid_str = "-" if not paid_at else datetime.fromtimestamp(paid_at).strftime("%Y-%m-%d %H:%M:%S")
    expire_str = "-" if not expires_at else datetime.fromtimestamp(expires_at).strftime("%Y-%m-%d %H:%M:%S")

    text = (
        f"🧾 <b>订单详情</b>\n\n"
        f"订单号：<code>{order_code}</code>\n"
        f"用户：<code>{user_id}</code> @{username or '-'}\n"
        f"姓名：{full_name or '-'}\n"
        f"类型：{category_title}\n"
        f"套餐：{plan_label}\n"
        f"金额：<b>{amount} U</b>\n"
        f"状态：<b>{status}</b>\n"
        f"创建时间：{created_str}\n"
        f"支付时间：{paid_str}\n"
        f"到期时间：{expire_str}\n"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ 确认已付款", callback_data=f"order:approve:{order_code}"),
            InlineKeyboardButton(text="❌ 拒绝", callback_data=f"order:reject:{order_code}"),
        ],
        [
            InlineKeyboardButton(text="⬅️ 返回订单列表", callback_data="order:list_pending")
        ]
    ])

    await c.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("order:approve:"))
async def order_approve_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return
    if not can_use_manage_panel(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    order_code = c.data.split(":", 2)[2]
    row = get_rental_order(order_code)
    if not row:
        return await c.answer("订单不存在", show_alert=True)

    (
        order_code, user_id, username, full_name, category_key, category_title,
        plan_key, plan_label, amount, status, created_at, paid_at, expires_at, note
    ) = row

    if status == "paid":
        return await c.answer("订单已支付", show_alert=True)

    row2, err = await activate_rental_order(order_code, granted_by=c.from_user.id)
    if err:
        return await c.answer(err, show_alert=True)

    new_expires_at = calc_renew_expire_at(user_id, plan_key)
    expire_str = datetime.fromtimestamp(new_expires_at).strftime("%Y-%m-%d %H:%M:%S")

    try:
        await bot.send_message(
            user_id,
            (
                "✅ <b>续费/租用成功</b>\n\n"
                f"订单号：<code>{order_code}</code>\n"
                f"类型：{category_title}\n"
                f"套餐：{plan_label}\n"
                f"到期时间：<b>{expire_str}</b>\n\n"
                "权限已自动开通/续期。"
            ),
            parse_mode="HTML"
        )
    except Exception as e:
        print("notify paid user failed:", e)

    await c.message.answer(
        (
            f"✅ <b>已确认付款</b>\n\n"
            f"订单号：<code>{order_code}</code>\n"
            f"用户：<code>{user_id}</code>\n"
            f"到期时间：<b>{expire_str}</b>\n"
            f"权限已开通/已续期。"
        ),
        parse_mode="HTML"
    )
    await c.answer("✅ 已开通/续期")

@dp.callback_query(lambda c: c.data and c.data.startswith("order:reject:"))
async def order_reject_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return
    if not can_use_manage_panel(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    order_code = c.data.split(":", 2)[2]
    row = get_rental_order(order_code)
    if not row:
        return await c.answer("订单不存在", show_alert=True)

    (
        order_code, user_id, username, full_name, category_key, category_title,
        plan_key, plan_label, amount, status, created_at, paid_at, expires_at, note
    ) = row

    if status == "paid":
        return await c.answer("订单已支付", show_alert=True)

    mark_rental_order_rejected(order_code)

    await c.message.answer(
        (
            f"❌ <b>订单已拒绝</b>\n\n"
            f"订单号：<code>{order_code}</code>\n"
            f"用户：<code>{user_id}</code>\n"
            f"套餐：{plan_label}\n"
            f"金额：<b>{amount} U</b>\n"
            f"状态：<b>rejected</b>"
        ),
        parse_mode="HTML"
    )

    try:
        await bot.send_message(
            user_id,
            (
                "❌ <b>您的订单未通过</b>\n\n"
                f"订单号：<code>{order_code}</code>\n"
                f"套餐：{plan_label}\n"
                "如有疑问，请联系管理员。"
            ),
            parse_mode="HTML"
        )
    except Exception as e:
        print("notify reject user failed:", e)

    await c.answer("✅ 已拒绝")

@dp.callback_query(lambda c: c.data == "order:list_pending")
async def order_list_pending_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return
    if not can_use_manage_panel(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    rows = get_pending_rental_orders(limit=10)
    if not rows:
        await c.message.answer("暂无待支付订单")
        return await c.answer()

    buttons = []
    for order_code, user_id, username, full_name, category_title, plan_label, amount, created_at in rows:
        buttons.append([
            InlineKeyboardButton(
                text=f"🧾 {order_code} | {plan_label} | {amount}U",
                callback_data=f"order:view:{order_code}"
            )
        ])

    await c.message.answer("🧾 <b>待支付订单</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await c.answer()

@dp.message(lambda m: m.text in ("订单历史", "租用历史", "历史订单"))
async def order_history_cmd(m: types.Message):
    if not can_use_manage_panel(m.from_user.id):
        return await m.reply("❌ 无权限")
    await m.reply("🧾 <b>订单历史</b>\n\n请选择查看类型：", reply_markup=order_history_kb(), parse_mode="HTML")

@dp.callback_query(lambda c: c.data and c.data.startswith("order:history:"))
async def order_history_cb(c: types.CallbackQuery):
    if not c.message or not c.from_user:
        return
    if not can_use_manage_panel(c.from_user.id):
        return await c.answer("无权限", show_alert=True)

    status = c.data.split(":")[2]
    if status == "all":
        rows = get_rental_orders_by_status(None, limit=20)
        title = "📦 全部订单"
    else:
        rows = get_rental_orders_by_status(status, limit=20)
        title = f"📦 {status}"

    if not rows:
        await c.message.answer(f"{title}\n\n暂无记录")
        return await c.answer()

    text = f"{title}\n\n"
    for row in rows:
        order_code, user_id, username, full_name, category_title, plan_label, amount, st, created_at, paid_at, expires_at = row
        created_str = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
        text += (
            f"• <code>{order_code}</code>\n"
            f"  {category_title} | {plan_label} | {amount}U | {st}\n"
            f"  用户：<code>{user_id}</code> @{username or '-'}\n"
            f"  时间：{created_str}\n\n"
        )

    await send_long_text(c.message.chat.id, text)
    await c.answer()

async def expiry_warning_loop():
    while True:
        try:
            now_ts = int(time.time())
            rows = get_access_users()

            for row in rows:
                user_id, username, granted_by, granted_at, expires_at = row
                if not expires_at:
                    continue

                expires_at = int(expires_at)
                remain = expires_at - now_ts

                if remain <= 0:
                    notice_key = "expired"
                    if not has_expiry_notice(user_id, notice_key):
                        add_expiry_notice(user_id, notice_key)
                        try:
                            await bot.send_message(user_id, "⏳ 您的使用权限已到期，请尽快续费。")
                        except Exception as e:
                            print("expired notify failed:", e)
                    continue

                warning_map = [
                    (7 * 24 * 3600, "7d", "7 天"),
                    (3 * 24 * 3600, "3d", "3 天"),
                    (1 * 24 * 3600, "1d", "1 天"),
                    (1 * 3600, "1h", "1 小时"),
                ]

                for threshold, key, label in warning_map:
                    if remain <= threshold and remain > threshold - 3600:
                        notice_key = f"warn_{key}"
                        if not has_expiry_notice(user_id, notice_key):
                            add_expiry_notice(user_id, notice_key)
                            try:
                                await bot.send_message(
                                    user_id,
                                    (
                                        f"⚠️ 您的权限将在 <b>{label}</b> 后到期。\n\n"
                                        f"到期时间：<code>{datetime.fromtimestamp(expires_at).strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
                                        f"请及时续费。"
                                    ),
                                    parse_mode="HTML",
                                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                        [InlineKeyboardButton(text="🔑 立即续费", callback_data="rent:main")]
                                    ])
                                )
                            except Exception as e:
                                print("warn notify failed:", e)
                        break

        except Exception as e:
            print("expiry_warning_loop error:", e)

        await asyncio.sleep(300)

# ================= HISTORY WEB =================
@dp.message(lambda m: m.text in ("交易历史", "📜 交易历史"))
async def menu_history(m: types.Message):
    if not can_use_bot_ops(m.from_user.id):
        return await m.reply("❌ 无权限")
    await m.reply(
        "📜 <b>交易历史</b>\n\n请选择一个群组，点击后将打开网页历史记录。",
        reply_markup=history_groups_kb(),
        parse_mode="HTML"
    )

# ================= REALTIME RATE =================
@dp.message(lambda m: m.text in ("实时U价", "📈 实时U价"))
async def menu_rate(m: types.Message):
    if not can_use_bot_ops(m.from_user.id):
        return await m.reply(deny_text())
    rates = await get_usdt_rates_cached()
    await m.answer(format_usdt_rate_text(rates), reply_markup=rate_kb(), parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "rate:refresh")
async def rate_refresh_cb(c: types.CallbackQuery):
    rates = await get_usdt_rates_cached(force=True)
    await c.message.answer(format_usdt_rate_text(rates), reply_markup=rate_kb(), parse_mode="HTML")
    await c.answer("✅ 已刷新")

# ================= ADDRESS QUERY =================
@dp.message(lambda m: m.text in ("地址查询", "🔍 地址查询", "📍 地址查询"))
async def menu_address_query(m: types.Message, state: FSMContext):
    if not can_use_bot_ops(m.from_user.id):
        return await m.reply("❌ 无权限")
    await state.set_state(AddressQueryFSM.waiting_address)
    await m.reply(address_query_text(), parse_mode="HTML")

@dp.message(AddressQueryFSM.waiting_address)
async def receive_address_query(m: types.Message, state: FSMContext):
    if not can_use_bot_ops(m.from_user.id):
        return await m.reply("❌ 无权限")

    addr = (m.text or "").strip()
    if not is_tron_address(addr):
        return await m.reply(
            "❌ 地址格式不正确，请重新输入 TRON 地址。\n"
            "示例：<code>TSPpLmYuFXLi6GU1W4uyG6NKGbdWPw886U</code>",
            parse_mode="HTML"
        )

    await m.reply("⏳ 正在查询链上数据，请稍候...")

    try:
        info = await check_tron_address(addr)
        text = format_address_info_text(addr, info)
    except Exception as e:
        print("on-chain query error:", e)
        text = f"🔎 查询地址：<code>{addr}</code>\n\n⚠️ 查询失败，请稍后再试。"

    await state.clear()
    await m.reply(text, parse_mode="HTML", reply_markup=address_result_kb(addr, page=1))

@dp.callback_query(lambda c: c.data == "addr:again")
async def addr_again_cb(c: types.CallbackQuery, state: FSMContext):
    if not c.message:
        return
    await state.set_state(AddressQueryFSM.waiting_address)
    await c.message.answer("🔍 <b>地址查询</b>\n\n请直接发送 TRON 地址进行查询。", parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data == "addr:back")
async def addr_back_cb(c: types.CallbackQuery, state: FSMContext):
    if not c.message:
        return
    await state.clear()
    await c.message.answer("✅ 已返回主菜单")
    await c.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("addr:tx:"))
async def addr_tx_cb(c: types.CallbackQuery):
    if not c.message:
        return

    parts = c.data.split(":")
    address = parts[2]
    page = int(parts[3]) if len(parts) >= 4 and parts[3].isdigit() else 1

    await c.message.answer("⏳ 正在加载交易记录，请稍候...")

    try:
        txs = await get_tron_transactions(address, page=page, page_size=10)
        if not txs:
            await c.message.answer(f"🔎 查询地址：<code>{address}</code>\n📄 当前页无交易记录", parse_mode="HTML")
            return await c.answer()

        text = f"🔎 查询地址：<code>{address}</code>\n🗂 当前页码：第 {page} 页\n\n📄 交易记录：\n"
        for tx in txs:
            text += format_tron_tx_row(tx) + "\n\n"

        await c.message.answer(text, parse_mode="HTML", reply_markup=tx_history_kb(address, page))
    except Exception as e:
        print("addr tx cb error:", e)
        await c.message.answer("⚠️ 交易记录加载失败，请稍后再试。")

    await c.answer()

# ================= MANAGE PANEL =================
@dp.message(lambda m: m.text in ("管理面板", "管理员快捷面板", "续费管理面板", "🛠 管理面板"))
async def manage_panel_cmd(m: types.Message):
    if not can_use_manage_panel(m.from_user.id):
        return await m.reply(deny_text())
    await m.reply("🛠 <b>管理面板</b>\n\n点击下方按钮执行操作。", reply_markup=manage_panel_kb(), parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "manage:list_admin")
async def manage_list_admin_cb(c: types.CallbackQuery):
    if not can_use_manage_panel(c.from_user.id):
        return await c.answer("无权限", show_alert=True)
    rows = get_all_admins()
    if not rows:
        await c.message.answer("暂无管理员")
        return await c.answer()
    text = "📋 <b>管理员列表</b>\n\n"
    for uid, role in rows:
        text += f"• <code>{uid}</code> — {role}\n"
    await c.message.answer(text, parse_mode="HTML")
    await c.answer()

@dp.callback_query(lambda c: c.data == "manage:create_code")
async def manage_create_code_cb(c: types.CallbackQuery, state: FSMContext):
    if not can_manage_codes(c.from_user.id):
        return await c.answer("无权限", show_alert=True)
    await state.set_state(AdminFSM.waiting_trial_code)
    await c.message.answer("🔑 <b>创建续费码</b>\n\n请发送新的续费码，例如：<code>ABC123</code>", parse_mode="HTML")
    await c.answer()

@dp.message(AdminFSM.waiting_trial_code)
async def receive_trial_code(m: types.Message, state: FSMContext):
    if not can_manage_codes(m.from_user.id):
        return await m.reply(deny_text())
    code = (m.text or "").strip()
    if not code:
        return await m.reply("❌ 请输入有效续费码。")
    set_trial_code(code)
    await state.clear()
    await m.reply(f"✅ 已设置续费码：<code>{code}</code>", parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "manage:add_admin")
async def manage_add_admin_cb(c: types.CallbackQuery, state: FSMContext):
    if not can_manage_admins(c.from_user.id):
        return await c.answer("无权限", show_alert=True)
    await state.set_state(AdminFSM.waiting_add_admin)
    await c.message.answer("➕ <b>添加管理员</b>\n\n请回复目标用户消息，或直接发送用户ID。", parse_mode="HTML")
    await c.answer()

@dp.message(AdminFSM.waiting_add_admin)
async def receive_add_admin(m: types.Message, state: FSMContext):
    if not can_manage_admins(m.from_user.id):
        return await m.reply(deny_text())
    uid = None
    if m.reply_to_message and m.reply_to_message.from_user:
        uid = m.reply_to_message.from_user.id
    elif m.text and m.text.strip().isdigit():
        uid = int(m.text.strip())
    if not uid:
        return await m.reply("❌ 格式错误，请回复某人消息或发送用户ID。")
    add_admin(uid, "admin")
    await state.clear()
    await m.reply(f"✅ 已添加管理员：<code>{uid}</code>", parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "manage:del_admin")
async def manage_del_admin_cb(c: types.CallbackQuery, state: FSMContext):
    if not can_manage_admins(c.from_user.id):
        return await c.answer("无权限", show_alert=True)
    await state.set_state(AdminFSM.waiting_del_admin)
    await c.message.answer("➖ <b>删除管理员</b>\n\n请回复目标用户消息，或直接发送用户ID。", parse_mode="HTML")
    await c.answer()

@dp.message(AdminFSM.waiting_del_admin)
async def receive_del_admin(m: types.Message, state: FSMContext):
    if not can_manage_admins(m.from_user.id):
        return await m.reply(deny_text())
    uid = None
    if m.reply_to_message and m.reply_to_message.from_user:
        uid = m.reply_to_message.from_user.id
    elif m.text and m.text.strip().isdigit():
        uid = int(m.text.strip())
    if not uid:
        return await m.reply("❌ 格式错误，请回复某人消息或发送用户ID。")
    remove_admin(uid)
    await state.clear()
    await m.reply(f"✅ 已删除管理员：<code>{uid}</code>", parse_mode="HTML")

@dp.callback_query(lambda c: c.data == "manage:revoke_code")
async def manage_revoke_code_cb(c: types.CallbackQuery):
    if not can_manage_codes(c.from_user.id):
        return await c.answer("无权限", show_alert=True)
    set_trial_code("")
    await c.message.answer("🗑 <b>续费码已回收</b>", parse_mode="HTML")
    await c.answer()

# ================= START / MENU =================
@dp.message(lambda m: m.text and is_cmd(m, "/start"))
async def start_cmd(m: types.Message):
    if not is_private(m):
        return
    text = get_setting(-1, "start_text") or main_menu_text()
    await m.answer(text, reply_markup=menu_kb(m.from_user.id), parse_mode="HTML")
    await m.answer("📋 常用命令复制区：", reply_markup=copy_cmd_kb(), disable_web_page_preview=True)

@dp.message(lambda m: m.text in ("🔥 开始记账", "开始记账", "开始"))
async def menu_begin(m: types.Message):
    if not is_private(m):
        return
    await m.answer(begin_help_text(), reply_markup=begin_copy_kb(), parse_mode="HTML")

@dp.message(lambda m: m.text in ("💎 申请试用", "申请试用"))
async def menu_trial(m: types.Message, state: FSMContext):
    if get_user_role(m.from_user.id) == "super":
        return await m.answer("🛠 管理员快捷面板", reply_markup=admin_copy_kb())

    if has_bot_access(m.from_user.id):
        return await m.reply("✅ 您已拥有使用权限。")

    if not has_claimed_free_trial(m.from_user.id):
        expires_at = int(time.time()) + 24 * 60 * 60
        add_access_user(
            user_id=m.from_user.id,
            username=m.from_user.username or "",
            granted_by=None,
            expires_at=expires_at
        )
        mark_claimed_free_trial(m.from_user.id)
        return await m.reply(
            "✅ 您已获得 24 小时免费试用权限。\n"
            "到期后请向管理员获取续费码。"
        )

    await state.set_state(TrialFSM.waiting_code)
    await m.reply(
        "⏳ 您的免费试用已用过或已到期。\n\n"
        "请输入管理员发送的续费码继续使用。"
    )

@dp.message(TrialFSM.waiting_code)
async def receive_trial_code(m: types.Message, state: FSMContext):
    if not m.text:
        return

    code = m.text.strip()
    real_code = (get_trial_code() or "").strip()

    if not real_code:
        return await m.reply("❌ 当前未设置续费码，请联系管理员。")

    if code != real_code:
        return await m.reply("❌ 续费码错误，请重试。")

    add_access_user(
        user_id=m.from_user.id,
        username=m.from_user.username or "",
        granted_by=None,
        expires_at=None
    )

    await state.clear()
    await m.reply("✅ 续费成功，您已获得长期使用权限。")

@dp.message(lambda m: m.text in ("📋 复制命令", "复制命令"))
async def menu_copy(m: types.Message):
    await m.reply("📋 常用命令复制区：", reply_markup=copy_cmd_kb())

@dp.callback_query(lambda c: c.data == "menu:copy")
async def menu_copy_cb(c: types.CallbackQuery):
    if not c.message:
        return
    await c.message.answer("📋 常用命令复制区：", reply_markup=copy_cmd_kb(), disable_web_page_preview=True)
    await c.answer()

@dp.message(lambda m: m.text in ("📝 使用说明", "使用说明", "/help"))
async def menu_help(m: types.Message):
    await m.reply(help_text(), reply_markup=menu_kb(m.from_user.id), parse_mode="HTML")

@dp.message(lambda m: m.text in ("🔑 自助续费", "自助续费"))
async def menu_renew(m: types.Message):
    if get_user_role(m.from_user.id) == "super":
        return await m.answer("🛠 续费管理面板", reply_markup=admin_copy_kb())
    await m.answer("🔑 自助续费请联系管理员。")

@dp.message(lambda m: m.text in ("📣 群发广播", "群发广播"))
async def menu_broadcast(m: types.Message, state: FSMContext):
    if is_private(m):
        if get_user_role(m.from_user.id) != "super":
            return await m.answer("❌ 只有超级管理员可在私聊里全局群发。")
        scope = "all"
        target_chat_id = -1
    else:
        ensure_group(m)
        if not can_use_manage_panel(m.from_user.id):
            return await m.reply("❌ 无权限")
        scope = "current"
        target_chat_id = m.chat.id

    await state.set_state(BroadcastFSM.waiting_content)
    await state.update_data(scope=scope, target_chat_id=target_chat_id, creator_id=m.from_user.id)
    await m.reply("📢 请发送要广播的内容。")

@dp.message(BroadcastFSM.waiting_content)
async def broadcast_receive_content(m: types.Message, state: FSMContext):
    data = await state.get_data()
    creator_id = data.get("creator_id")

    if creator_id and m.from_user and m.from_user.id != creator_id:
        return

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
    creator_id = data.get("creator_id")

    if creator_id and c.from_user.id != creator_id:
        return await c.answer("❌ 无权限", show_alert=True)

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
                await bot.copy_message(chat_id=chat_id, from_chat_id=source_chat_id, message_id=source_message_id)
            else:
                await bot.forward_message(chat_id=chat_id, from_chat_id=source_chat_id, message_id=source_message_id)
            ok += 1
        except Exception as e:
            fail += 1
            print("broadcast error:", e)

    await state.clear()
    await c.message.edit_text(f"✅ 群发完成\n成功：{ok}\n失败：{fail}")

@dp.message(lambda m: m.text in ("📜 交易历史", "交易历史"))
async def menu_history(m: types.Message):
    if not can_use_bot_ops(m.from_user.id):
        return await m.reply("❌ 无权限")
    await m.reply(
        "📜 <b>交易历史</b>\n\n请选择一个群组，点击后将打开网页历史记录。",
        reply_markup=history_groups_kb(),
        parse_mode="HTML"
    )

# ================= TRON CHECK =================
@dp.message(lambda m: m.text and extract_tron_address(m.text) is not None)
async def tron_address_check_handler(m: types.Message):
    if should_ignore_message(m):
        return

    address = extract_tron_address(m.text)
    if not address:
        return

    status_msg = await m.reply("⏳ 正在查询地址，请稍候...")

    try:
        info = await check_tron_address(address)
        if not info:
            return await status_msg.edit_text("❌ 未能获取钱包数据，请稍后再试。")

        now_ts = int(time.time())
        warnings = []

        if info["tx_count"] == 0:
            warnings.append("该地址暂无交易记录。")

        if info["trx_balance"] is not None and info["trx_balance"] < 1:
            warnings.append("TRX余额较低，可能影响链上操作。")

        if info["latest_time"]:
            try:
                lt = int(info["latest_time"])
                if lt > 10_000_000_000:
                    lt = lt // 1000
                if now_ts - lt > 30 * 24 * 3600:
                    warnings.append("该地址已较长时间未活跃。")
            except:
                pass

        add_wallet_check(
            chat_id=m.chat.id,
            user_id=m.from_user.id,
            username=m.from_user.username or "",
            full_name=m.from_user.full_name or "",
            address=address,
            trx_balance=info["trx_balance"],
            usdt_balance=info["usdt_balance"],
            tx_count=info["tx_count"]
        )

        sender_name = m.from_user.full_name or (m.from_user.username or "Unknown")

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔗 关联账单",
                    url=f"https://tronscan.org/#/address/{address}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="📄 最近交易",
                    callback_data="wallet:recent:0"
                )
            ]
        ])

        caption = (
            f"🔎 TRON 地址查询\n"
            f"• 查询人：{sender_name}\n"
            f"• 地址：`{address}`\n"
            f"• TRX：`{fmt_num(info['trx_balance'])}`\n"
            f"• USDT：`{fmt_num(info['usdt_balance'])}`\n"
            f"• 交易次数：`{info['tx_count'] if info['tx_count'] is not None else 'N/A'}`"
        )

        if warnings:
            caption += "\n\n⚠️ 风险提示：\n" + "\n".join([f"• {w}" for w in warnings])

        try:
            photo = make_wallet_card_image(
                address=address,
                sender_name=sender_name,
                trx_balance=info["trx_balance"],
                usdt_balance=info["usdt_balance"],
                tx_count=info["tx_count"],
                source=info["source"],
                create_time=info.get("create_time"),
                latest_time=info.get("latest_time"),
            )

            await m.answer_photo(
                photo=photo,
                caption=caption,
                reply_markup=kb,
                parse_mode="Markdown"
            )
        except Exception as e:
            print("send wallet photo error:", e)
            await m.reply(caption, reply_markup=kb, parse_mode="Markdown")

    except Exception as e:
        print("tron_address_check_handler error:", e)
        try:
            await status_msg.edit_text("❌ 查询地址时发生错误。")
        except:
            pass

    try:
        await status_msg.delete()
    except:
        pass

# ================= WALLET CHECKS =================
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
        tm = fmt_ts(created_at)

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
        tm = fmt_ts(created_at)

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

# ================= REPORT =================
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

# ================= LEDGER HANDLER =================
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
