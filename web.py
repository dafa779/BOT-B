import os
from html import escape
from datetime import datetime, timedelta

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from dotenv import load_dotenv

from db import init_db, get_groups, get_transactions, summarize_transactions

# ================= ENV =================
load_dotenv()
WEB_ADMIN_TOKEN = os.getenv("WEB_ADMIN_TOKEN", "").strip()

# ================= APP =================
init_db()
app = FastAPI(title="Ledger Web", version="1.0.0")


# ================= AUTH OPTIONAL =================
def check_token(token: str | None):
    """
    Nếu WEB_ADMIN_TOKEN trống thì cho xem tự do.
    Nếu có token thì phải truyền ?token=...
    """
    if not WEB_ADMIN_TOKEN:
        return True
    return token == WEB_ADMIN_TOKEN


def require_token(token: str | None):
    if not check_token(token):
        raise HTTPException(status_code=401, detail="Unauthorized")


# ================= HELPERS =================
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


def parse_web_date(date_str: str | None):
    """
    date_str: YYYY-MM-DD
    """
    if not date_str:
        dt = datetime.now()
    else:
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except:
            dt = datetime.now()

    start = datetime(dt.year, dt.month, dt.day)
    end = start + timedelta(days=1) - timedelta(seconds=1)

    return {
        "date_str": start.strftime("%Y-%m-%d"),
        "start_ts": int(start.timestamp()),
        "end_ts": int(end.timestamp()),
        "start_dt": start,
        "end_dt": end,
    }


def get_group_title_map():
    return {int(chat_id): title for chat_id, title in get_groups()}


def render_groups_page(token: str | None = None):
    groups = get_groups()

    today = datetime.now().strftime("%Y-%m-%d")
    rows = ""

    for chat_id, title in groups:
        rows += f"""
        <tr>
            <td>{escape(title or 'Unnamed')}</td>
            <td><span class="tag">{chat_id}</span></td>
            <td>
                <a class="btn" href="/group/{chat_id}?date={today}{f'&token={escape(token)}' if token else ''}">
                    Xem lịch sử
                </a>
            </td>
        </tr>
        """

    if not rows:
        rows = """
        <tr>
            <td colspan="3" style="text-align:center;color:#9ca3af;">Chưa có nhóm nào</td>
        </tr>
        """

    token_q = f"?token={token}" if token else ""

    html = f"""
    <!doctype html>
    <html lang="vi">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Danh sách nhóm</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background: #0f172a;
                color: #e5e7eb;
                margin: 0;
                padding: 20px;
            }}
            .container {{
                max-width: 1100px;
                margin: auto;
                background: #111827;
                border-radius: 16px;
                padding: 20px;
                box-shadow: 0 10px 30px rgba(0,0,0,.35);
            }}
            h1 {{
                margin: 0 0 10px 0;
            }}
            .muted {{
                color: #9ca3af;
                font-size: 14px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 16px;
            }}
            th, td {{
                border-bottom: 1px solid #374151;
                padding: 10px;
                text-align: left;
            }}
            th {{
                background: #1f2937;
            }}
            .btn {{
                display: inline-block;
                padding: 8px 12px;
                border-radius: 10px;
                background: #2563eb;
                color: #fff;
                text-decoration: none;
            }}
            .tag {{
                display: inline-block;
                padding: 4px 8px;
                border-radius: 999px;
                background: #374151;
                color: #fff;
                font-size: 12px;
            }}
            a {{
                color: #60a5fa;
                text-decoration: none;
            }}
            a:hover {{
                text-decoration: underline;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📋 Danh sách nhóm</h1>
            <div class="muted">
                Chọn nhóm để xem lịch sử giao dịch riêng theo từng group.
            </div>

            <table>
                <thead>
                    <tr>
                        <th>Tên nhóm</th>
                        <th>Chat ID</th>
                        <th>Hành động</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(html)


def render_group_history_page(chat_id: int, date_str: str | None = None, token: str | None = None):
    groups = get_group_title_map()
    group_title = groups.get(int(chat_id), f"Group {chat_id}")

    day = parse_web_date(date_str)

    txs = get_transactions(
        chat_id,
        start_ts=day["start_ts"],
        end_ts=day["end_ts"]
    )
    stats = summarize_transactions(txs)

    income_txs = [t for t in txs if t[6] == "income"]
    payout_txs = [t for t in txs if t[6] == "payout"]
    reserve_txs = [t for t in txs if t[6] == "reserve"]

    prev_day = (day["start_dt"] - timedelta(days=1)).strftime("%Y-%m-%d")
    next_day = (day["start_dt"] + timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")

    token_param = f"&token={token}" if token else ""
    token_input = f'<input type="hidden" name="token" value="{escape(token)}">' if token else ""

    rows_html = ""
    if txs:
        for tx in txs:
            tx_id, c_id, user_id, username, display_name, target_name, kind, raw_amount, unit_amount, rate_used, fee_used, note, original_text, created_at, undone = tx
            tm = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
            status = "✅" if not undone else "↩️"

            rows_html += f"""
            <tr>
                <td>{escape(tm)}</td>
                <td>{escape(kind or '')}</td>
                <td>{escape(fmt_num(raw_amount) if raw_amount is not None else '-')}</td>
                <td>{escape(fmt_num(unit_amount))}U</td>
                <td>{escape(fmt_num(rate_used))}</td>
                <td>{escape(fmt_num(fee_used))}%</td>
                <td>{escape(display_name or '')}</td>
                <td>{escape(target_name or '')}</td>
                <td>{escape(note or '')}</td>
                <td>{escape(original_text or '')}</td>
                <td>{status}</td>
            </tr>
            """
    else:
        rows_html = """
        <tr>
            <td colspan="11" style="text-align:center;color:#9ca3af;">Ngày này chưa có giao dịch</td>
        </tr>
        """

    html = f"""
    <!doctype html>
    <html lang="vi">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{escape(group_title)} - Lịch sử giao dịch</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background: #0f172a;
                color: #e5e7eb;
                margin: 0;
                padding: 20px;
            }}
            .container {{
                max-width: 1600px;
                margin: auto;
                background: #111827;
                border-radius: 16px;
                padding: 20px;
                box-shadow: 0 10px 30px rgba(0,0,0,.35);
            }}
            .topbar {{
                display: flex;
                justify-content: space-between;
                gap: 12px;
                align-items: center;
                flex-wrap: wrap;
                margin-bottom: 16px;
            }}
            h1, h2 {{
                margin: 0 0 10px 0;
            }}
            .muted {{
                color: #9ca3af;
                font-size: 14px;
                line-height: 1.6;
            }}
            .tag {{
                display: inline-block;
                padding: 4px 8px;
                border-radius: 999px;
                background: #2563eb;
                color: white;
                font-size: 12px;
            }}
            .stats {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 12px;
                margin: 18px 0;
            }}
            .card {{
                background: #1f2937;
                border-radius: 12px;
                padding: 14px;
                border: 1px solid #374151;
            }}
            .card .label {{
                color: #9ca3af;
                font-size: 13px;
            }}
            .card .value {{
                font-size: 22px;
                font-weight: 700;
                margin-top: 6px;
            }}
            .filters {{
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
                align-items: center;
                margin: 14px 0 6px;
            }}
            input[type="date"] {{
                background: #111827;
                color: #e5e7eb;
                border: 1px solid #374151;
                padding: 10px 12px;
                border-radius: 10px;
            }}
            .btn {{
                display: inline-block;
                padding: 10px 14px;
                border-radius: 10px;
                background: #2563eb;
                color: #fff;
                text-decoration: none;
                border: 0;
                cursor: pointer;
            }}
            .btn.secondary {{
                background: #374151;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 14px;
                overflow: hidden;
                border-radius: 12px;
            }}
            th, td {{
                border-bottom: 1px solid #374151;
                padding: 10px 8px;
                font-size: 14px;
                text-align: left;
                vertical-align: top;
                white-space: nowrap;
            }}
            th {{
                background: #1f2937;
                color: #f9fafb;
                position: sticky;
                top: 0;
                z-index: 1;
            }}
            tr:hover td {{
                background: rgba(255,255,255,0.03);
            }}
            .table-wrap {{
                overflow-x: auto;
            }}
            a {{
                color: #60a5fa;
                text-decoration: none;
            }}
            a:hover {{
                text-decoration: underline;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="topbar">
                <div>
                    <h1>📘 Lịch sử giao dịch nhóm</h1>
                    <div class="muted">
                        Nhóm: <b>{escape(group_title)}</b> |
                        Chat ID: <span class="tag">{chat_id}</span> |
                        Ngày: <span class="tag">{day["date_str"]}</span>
                    </div>
                </div>
                <div>
                    <a class="btn secondary" href="/groups{('?token=' + escape(token)) if token else ''}">← Danh sách nhóm</a>
                </div>
            </div>

            <form class="filters" method="get" action="/group/{chat_id}">
                {token_input}
                <label for="date">Chọn ngày:</label>
                <input type="date" id="date" name="date" value="{day["date_str"]}">
                <button class="btn" type="submit">Xem</button>
                <a class="btn secondary" href="/group/{chat_id}?date={today}{token_param}">Hôm nay</a>
                <a class="btn secondary" href="/group/{chat_id}?date={prev_day}{token_param}">Hôm qua</a>
                <a class="btn secondary" href="/group/{chat_id}?date={next_day}{token_param}">Ngày mai</a>
            </form>

            <div class="stats">
                <div class="card">
                    <div class="label">Tổng giao dịch</div>
                    <div class="value">{len(txs)}</div>
                </div>
                <div class="card">
                    <div class="label">Tổng nhập</div>
                    <div class="value">{fmt_num(stats["total_income_unit"])}U</div>
                </div>
                <div class="card">
                    <div class="label">Tổng đã下发</div>
                    <div class="value">{fmt_num(stats["total_payout_unit"])}U</div>
                </div>
                <div class="card">
                    <div class="label">Tổng寄存</div>
                    <div class="value">{fmt_num(stats["total_reserve_unit"])}U</div>
                </div>
                <div class="card">
                    <div class="label">Còn pending</div>
                    <div class="value">{fmt_num(stats["pending"])}U</div>
                </div>
                <div class="card">
                    <div class="label">Số nhập</div>
                    <div class="value">{len(income_txs)}</div>
                </div>
                <div class="card">
                    <div class="label">Số下发</div>
                    <div class="value">{len(payout_txs)}</div>
                </div>
                <div class="card">
                    <div class="label">Số寄存</div>
                    <div class="value">{len(reserve_txs)}</div>
                </div>
            </div>

            <div class="muted">
                Dữ liệu từ <b>{day["start_dt"].strftime("%Y-%m-%d 00:00:00")}</b>
                đến <b>{day["end_dt"].strftime("%Y-%m-%d 23:59:59")}</b>
            </div>

            <h2 style="margin-top:16px;">Danh sách giao dịch</h2>
            <div class="table-wrap">
                <table>
                    <thead>
                        <tr>
                            <th>Thời gian</th>
                            <th>Loại</th>
                            <th>Raw</th>
                            <th>U</th>
                            <th>Rate</th>
                            <th>Fee</th>
                            <th>Người ghi</th>
                            <th>Target</th>
                            <th>Ghi chú</th>
                            <th>Original</th>
                            <th>TT</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html}
                    </tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(html)


# ================= ROUTES =================
@app.get("/", include_in_schema=False)
def home():
    return RedirectResponse(url="/groups")


@app.get("/groups", response_class=HTMLResponse)
def groups_page(token: str | None = Query(default=None)):
    require_token(token)
    return render_groups_page(token=token)


@app.get("/group/{chat_id}", response_class=HTMLResponse)
def group_history(chat_id: int, date: str | None = Query(default=None), token: str | None = Query(default=None)):
    require_token(token)
    return render_group_history_page(chat_id, date_str=date, token=token)


@app.get("/healthz")
def healthz():
    return {"ok": True}
