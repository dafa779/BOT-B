import os
import time

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
PGSSLMODE = os.getenv("PGSSLMODE", "").strip()


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")

    kwargs = {
        "connect_timeout": 30,
    }

    if PGSSLMODE:
        kwargs["sslmode"] = PGSSLMODE

    return psycopg2.connect(DATABASE_URL, **kwargs)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS admins (
        user_id BIGINT PRIMARY KEY,
        role TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS groups (
        chat_id BIGINT PRIMARY KEY,
        name TEXT,
        updated_at BIGINT DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        chat_id BIGINT,
        key TEXT,
        value TEXT,
        PRIMARY KEY(chat_id, key)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS operators (
        id BIGSERIAL PRIMARY KEY,
        chat_id BIGINT,
        user_id BIGINT,
        username TEXT,
        role TEXT DEFAULT 'operator',
        UNIQUE(chat_id, user_id),
        UNIQUE(chat_id, username)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS members (
        chat_id BIGINT,
        user_id BIGINT,
        username TEXT,
        name TEXT,
        last_seen BIGINT,
        PRIMARY KEY(chat_id, user_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id BIGSERIAL PRIMARY KEY,
        chat_id BIGINT,
        user_id BIGINT,
        username TEXT,
        display_name TEXT,
        target_name TEXT,
        kind TEXT,
        raw_amount DOUBLE PRECISION,
        unit_amount DOUBLE PRECISION,
        rate_used DOUBLE PRECISION,
        fee_used DOUBLE PRECISION,
        note TEXT,
        original_text TEXT,
        created_at BIGINT,
        undone BOOLEAN DEFAULT FALSE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS access_users (
        user_id BIGINT PRIMARY KEY,
        username TEXT,
        granted_by BIGINT,
        granted_at BIGINT,
        expires_at BIGINT,
        reminder_1h_sent BOOLEAN DEFAULT FALSE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS trial_claims (
        user_id BIGINT PRIMARY KEY,
        username TEXT DEFAULT '',
        claimed_at BIGINT NOT NULL
    )
    """)

        cur.execute("""
    CREATE TABLE IF NOT EXISTS wallet_checks (
        id BIGSERIAL PRIMARY KEY,
        chat_id BIGINT,
        user_id BIGINT,
        username TEXT,
        full_name TEXT,
        address TEXT,
        trx_balance DOUBLE PRECISION,
        usdt_balance DOUBLE PRECISION,
        tx_count INTEGER,
        created_at BIGINT NOT NULL
    )
    """)
    
    conn.commit()
    cur.close()
    conn.close()


# ================= ADMIN =================
def add_admin(user_id, role="admin"):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO admins(user_id, role)
    VALUES (%s, %s)
    ON CONFLICT(user_id) DO UPDATE SET role=EXCLUDED.role
    """, (user_id, role))
    conn.commit()
    cur.close()
    conn.close()


def remove_admin(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM admins WHERE user_id=%s", (user_id,))
    conn.commit()
    cur.close()
    conn.close()


def get_admin(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT role FROM admins WHERE user_id=%s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None


def get_all_admins():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id, role FROM admins ORDER BY user_id ASC")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ================= GROUP =================
def save_group(chat_id, name):
    conn = get_conn()
    cur = conn.cursor()
    now_ts = int(time.time())
    cur.execute("""
    INSERT INTO groups(chat_id, name, updated_at)
    VALUES (%s, %s, %s)
    ON CONFLICT(chat_id) DO UPDATE SET
        name=EXCLUDED.name,
        updated_at=EXCLUDED.updated_at
    """, (chat_id, name, now_ts))
    conn.commit()
    cur.close()
    conn.close()


def get_groups():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT chat_id, name
    FROM groups
    ORDER BY updated_at DESC, chat_id ASC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ================= SETTINGS =================
def set_setting(chat_id, key, value):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO settings(chat_id, key, value)
    VALUES (%s, %s, %s)
    ON CONFLICT(chat_id, key) DO UPDATE SET value=EXCLUDED.value
    """, (chat_id, key, str(value)))
    conn.commit()
    cur.close()
    conn.close()


def get_setting(chat_id, key, default=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT value FROM settings
    WHERE chat_id=%s AND key=%s
    """, (chat_id, key))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else default


def delete_setting(chat_id, key):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM settings WHERE chat_id=%s AND key=%s", (chat_id, key))
    conn.commit()
    cur.close()
    conn.close()


def set_button_config(chat_id, idx, text, url):
    set_setting(chat_id, f"btn{idx}_text", text)
    set_setting(chat_id, f"btn{idx}_url", url)


def get_button_config(chat_id, idx):
    text = get_setting(chat_id, f"btn{idx}_text", "")
    url = get_setting(chat_id, f"btn{idx}_url", "")
    return text, url


def get_all_button_configs(chat_id):
    data = []
    for i in range(1, 5):
        text, url = get_button_config(chat_id, i)
        if text and url:
            data.append((text, url))
    return data


# ================= OPERATORS =================
def add_operator(chat_id, user_id=None, username=None, role="operator"):
    conn = get_conn()
    cur = conn.cursor()

    if user_id is not None:
        cur.execute("""
        INSERT INTO operators(chat_id, user_id, username, role)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT(chat_id, user_id) DO UPDATE SET
            username=EXCLUDED.username,
            role=EXCLUDED.role
        """, (chat_id, user_id, username, role))

    elif username:
        cur.execute("""
        INSERT INTO operators(chat_id, user_id, username, role)
        VALUES (%s, NULL, %s, %s)
        ON CONFLICT(chat_id, username) DO UPDATE SET
            role=EXCLUDED.role
        """, (chat_id, username, role))

    conn.commit()
    cur.close()
    conn.close()


def remove_operator(chat_id, user_id=None, username=None):
    conn = get_conn()
    cur = conn.cursor()

    if user_id is not None:
        cur.execute("DELETE FROM operators WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
    elif username is not None:
        cur.execute("DELETE FROM operators WHERE chat_id=%s AND username=%s", (chat_id, username))

    conn.commit()
    cur.close()
    conn.close()


def clear_operators(chat_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM operators WHERE chat_id=%s", (chat_id,))
    conn.commit()
    cur.close()
    conn.close()


def get_operators(chat_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT user_id, username, role
    FROM operators
    WHERE chat_id=%s
    ORDER BY id ASC
    """, (chat_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_global_operators():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT user_id, username, role
    FROM operators
    WHERE chat_id=-1
    ORDER BY id ASC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def is_operator(chat_id, user_id=None, username=None):
    rows = []
    rows.extend(get_operators(chat_id))
    rows.extend(get_global_operators())

    for uid, uname, role in rows:
        if user_id is not None and uid is not None and int(uid) == int(user_id):
            return True
        if username and uname and uname.lower() == username.lower():
            return True
    return False


# ================= MEMBERS =================
def save_member(chat_id, user_id, username, name):
    conn = get_conn()
    cur = conn.cursor()
    now_ts = int(time.time())
    cur.execute("""
    INSERT INTO members(chat_id, user_id, username, name, last_seen)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(chat_id, user_id) DO UPDATE SET
        username=EXCLUDED.username,
        name=EXCLUDED.name,
        last_seen=EXCLUDED.last_seen
    """, (chat_id, user_id, username, name, now_ts))
    conn.commit()
    cur.close()
    conn.close()


def get_members(chat_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT chat_id, user_id, username, name, last_seen
    FROM members
    WHERE chat_id=%s
    ORDER BY last_seen DESC, user_id ASC
    """, (chat_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ================= TRANSACTIONS =================
def add_transaction(
    chat_id,
    user_id,
    username,
    display_name,
    target_name,
    kind,
    raw_amount,
    unit_amount,
    rate_used,
    fee_used,
    note,
    original_text
):
    conn = get_conn()
    cur = conn.cursor()
    created_at = int(time.time())

    cur.execute("""
    INSERT INTO transactions(
        chat_id, user_id, username, display_name, target_name, kind,
        raw_amount, unit_amount, rate_used, fee_used, note, original_text,
        created_at, undone
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE)
    RETURNING id
    """, (
        chat_id, user_id, username, display_name, target_name, kind,
        raw_amount, unit_amount, rate_used, fee_used, note, original_text,
        created_at
    ))

    tx_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return tx_id


def get_transaction(tx_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT id, chat_id, user_id, username, display_name, target_name, kind,
           raw_amount, unit_amount, rate_used, fee_used, note, original_text,
           created_at, undone
    FROM transactions
    WHERE id=%s
    """, (tx_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def get_last_transaction(chat_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT id, chat_id, user_id, username, display_name, target_name, kind,
           raw_amount, unit_amount, rate_used, fee_used, note, original_text,
           created_at, undone
    FROM transactions
    WHERE chat_id=%s AND undone=FALSE
    ORDER BY created_at DESC, id DESC
    LIMIT 1
    """, (chat_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def undo_transaction(tx_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE transactions SET undone=TRUE WHERE id=%s", (tx_id,))
    conn.commit()
    cur.close()
    conn.close()


def clear_transactions(chat_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM transactions WHERE chat_id=%s", (chat_id,))
    conn.commit()
    cur.close()
    conn.close()


def get_transactions(chat_id, start_ts=None, end_ts=None, user_id=None, keyword=None, include_undone=False):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
    SELECT id, chat_id, user_id, username, display_name, target_name, kind,
           raw_amount, unit_amount, rate_used, fee_used, note, original_text,
           created_at, undone
    FROM transactions
    WHERE chat_id=%s
    """
    params = [chat_id]

    if not include_undone:
        sql += " AND undone=FALSE"

    if start_ts is not None:
        sql += " AND created_at >= %s"
        params.append(int(start_ts))

    if end_ts is not None:
        sql += " AND created_at <= %s"
        params.append(int(end_ts))

    if user_id is not None:
        sql += " AND user_id = %s"
        params.append(int(user_id))

    if keyword:
        sql += """
        AND (
            display_name ILIKE %s OR
            target_name ILIKE %s OR
            username ILIKE %s OR
            note ILIKE %s OR
            original_text ILIKE %s
        )
        """
        kw = f"%{keyword}%"
        params.extend([kw, kw, kw, kw, kw])

    sql += " ORDER BY created_at ASC, id ASC"

    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ================= TRIAL / ACCESS =================
def set_trial_code(code):
    set_setting(-1, "trial_code", code)


def get_trial_code():
    return get_setting(-1, "trial_code", "")


def has_trial_claimed(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM trial_claims WHERE user_id=%s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row is not None


def mark_trial_claimed(user_id, username=""):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO trial_claims(user_id, username, claimed_at)
    VALUES (%s, %s, %s)
    ON CONFLICT(user_id) DO UPDATE SET
        username=EXCLUDED.username,
        claimed_at=EXCLUDED.claimed_at
    """, (user_id, username or "", int(time.time())))
    conn.commit()
    cur.close()
    conn.close()


def add_access_user(user_id, username="", granted_by=None, expires_at=None):
    conn = get_conn()
    cur = conn.cursor()
    now_ts = int(time.time())
    cur.execute("""
    INSERT INTO access_users(user_id, username, granted_by, granted_at, expires_at, reminder_1h_sent)
    VALUES (%s, %s, %s, %s, %s, FALSE)
    ON CONFLICT(user_id) DO UPDATE SET
        username=EXCLUDED.username,
        granted_by=EXCLUDED.granted_by,
        granted_at=EXCLUDED.granted_at,
        expires_at=EXCLUDED.expires_at,
        reminder_1h_sent=FALSE
    """, (user_id, username or "", granted_by, now_ts, expires_at))
    conn.commit()
    cur.close()
    conn.close()


def remove_access_user(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM access_users WHERE user_id=%s", (user_id,))
    conn.commit()
    cur.close()
    conn.close()


def has_access_user(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT expires_at FROM access_users WHERE user_id=%s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return False

    expires_at = row[0]
    if expires_at is None:
        return True

    return int(time.time()) < int(expires_at)


def get_access_users():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT user_id, username, granted_by, granted_at, expires_at
    FROM access_users
    ORDER BY granted_at DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_expired_access_users(now_ts=None):
    if now_ts is None:
        now_ts = int(time.time())

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT user_id, username, expires_at
    FROM access_users
    WHERE expires_at IS NOT NULL AND expires_at <= %s
    """, (now_ts,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_access_users_expiring_soon(now_ts=None, within_seconds=3600):
    if now_ts is None:
        now_ts = int(time.time())

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT user_id, username, granted_by, granted_at, expires_at, reminder_1h_sent
    FROM access_users
    WHERE expires_at IS NOT NULL
      AND expires_at > %s
      AND expires_at <= %s
      AND reminder_1h_sent = FALSE
    """, (now_ts, now_ts + within_seconds))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def mark_access_reminded_1h(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    UPDATE access_users
    SET reminder_1h_sent = TRUE
    WHERE user_id=%s
    """, (user_id,))
    conn.commit()
    cur.close()
    conn.close()

def add_wallet_check(chat_id, user_id, username, full_name, address, trx_balance, usdt_balance, tx_count):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO wallet_checks(
        chat_id, user_id, username, full_name, address,
        trx_balance, usdt_balance, tx_count, created_at
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        chat_id,
        user_id,
        username or "",
        full_name or "",
        address or "",
        trx_balance,
        usdt_balance,
        tx_count,
        int(time.time())
    ))
    conn.commit()
    cur.close()
    conn.close()


def get_wallet_checks_page(limit=10, offset=0):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT id, chat_id, user_id, username, full_name, address,
           trx_balance, usdt_balance, tx_count, created_at
    FROM wallet_checks
    ORDER BY id DESC
    LIMIT %s OFFSET %s
    """, (limit, offset))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def count_wallet_checks():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM wallet_checks")
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else 0
