import sqlite3
import time

DB_NAME = "data.db"


def get_conn():
    conn = sqlite3.connect(DB_NAME, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS admins (
        user_id INTEGER PRIMARY KEY,
        role TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS groups (
        chat_id INTEGER PRIMARY KEY,
        name TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        chat_id INTEGER,
        key TEXT,
        value TEXT,
        PRIMARY KEY(chat_id, key)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS operators (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        user_id INTEGER,
        username TEXT,
        role TEXT DEFAULT 'operator',
        UNIQUE(chat_id, user_id),
        UNIQUE(chat_id, username)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS members (
        chat_id INTEGER,
        user_id INTEGER,
        username TEXT,
        name TEXT,
        last_seen INTEGER,
        PRIMARY KEY(chat_id, user_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        user_id INTEGER,
        username TEXT,
        display_name TEXT,
        target_name TEXT,
        kind TEXT,
        raw_amount REAL,
        unit_amount REAL,
        rate_used REAL,
        fee_used REAL,
        note TEXT,
        original_text TEXT,
        created_at INTEGER,
        undone INTEGER DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS access_users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        granted_by INTEGER,
        granted_at INTEGER,
        expires_at INTEGER
    )
    """)

    # Migrations
    try:
        cur.execute("ALTER TABLE access_users ADD COLUMN expires_at INTEGER")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()


# ================= ADMIN =================
def add_admin(user_id, role="admin"):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO admins(user_id, role)
    VALUES (?, ?)
    ON CONFLICT(user_id) DO UPDATE SET role=excluded.role
    """, (user_id, role))
    conn.commit()
    conn.close()


def remove_admin(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM admins WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()


def get_admin(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT role FROM admins WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def get_all_admins():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id, role FROM admins ORDER BY user_id ASC")
    rows = cur.fetchall()
    conn.close()
    return rows


# ================= GROUP =================
def save_group(chat_id, name):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO groups(chat_id, name)
    VALUES (?, ?)
    ON CONFLICT(chat_id) DO UPDATE SET name=excluded.name
    """, (chat_id, name))
    conn.commit()
    conn.close()


def get_groups():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT chat_id, name FROM groups ORDER BY chat_id ASC")
    rows = cur.fetchall()
    conn.close()
    return rows


# ================= SETTINGS =================
def set_setting(chat_id, key, value):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO settings(chat_id, key, value)
    VALUES (?, ?, ?)
    ON CONFLICT(chat_id, key) DO UPDATE SET value=excluded.value
    """, (chat_id, key, str(value)))
    conn.commit()
    conn.close()


def get_setting(chat_id, key, default=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT value FROM settings
    WHERE chat_id=? AND key=?
    """, (chat_id, key))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else default


def delete_setting(chat_id, key):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM settings WHERE chat_id=? AND key=?", (chat_id, key))
    conn.commit()
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
    cur.execute("""
    INSERT INTO operators(chat_id, user_id, username, role)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(chat_id, user_id) DO UPDATE SET
        username=excluded.username,
        role=excluded.role
    """, (chat_id, user_id, username, role))
    conn.commit()
    conn.close()


def remove_operator(chat_id, user_id=None, username=None):
    conn = get_conn()
    cur = conn.cursor()
    if user_id is not None:
        cur.execute("DELETE FROM operators WHERE chat_id=? AND user_id=?", (chat_id, user_id))
    elif username is not None:
        cur.execute("DELETE FROM operators WHERE chat_id=? AND username=?", (chat_id, username))
    conn.commit()
    conn.close()


def clear_operators(chat_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM operators WHERE chat_id=?", (chat_id,))
    conn.commit()
    conn.close()


def get_operators(chat_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT user_id, username, role
    FROM operators
    WHERE chat_id=?
    ORDER BY id ASC
    """, (chat_id,))
    rows = cur.fetchall()
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
    conn.close()
    return rows


def is_operator(chat_id, user_id=None, username=None):
    rows = []
    rows.extend(get_operators(chat_id))
    rows.extend(get_global_operators())

    for uid, uname, role in rows:
        if user_id is not None and uid is not None and uid == user_id:
            return True
        if username and uname and uname.lower() == username.lower():
            return True
    return False


# ================= MEMBERS =================
def save_member(chat_id, user_id, username, name):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO members(chat_id, user_id, username, name, last_seen)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(chat_id, user_id) DO UPDATE SET
        username=excluded.username,
        name=excluded.name,
        last_seen=excluded.last_seen
    """, (chat_id, user_id, username, name, int(time.time())))
    conn.commit()
    conn.close()


def get_members(chat_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT chat_id, user_id, username, name, last_seen
    FROM members
    WHERE chat_id=?
    """, (chat_id,))
    rows = cur.fetchall()
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
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,0)
    """, (
        chat_id, user_id, username, display_name, target_name, kind,
        raw_amount, unit_amount, rate_used, fee_used, note, original_text,
        created_at
    ))
    tx_id = cur.lastrowid
    conn.commit()
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
    WHERE id=?
    """, (tx_id,))
    row = cur.fetchone()
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
    WHERE chat_id=? AND undone=0
    ORDER BY created_at DESC, id DESC
    LIMIT 1
    """, (chat_id,))
    row = cur.fetchone()
    conn.close()
    return row


def undo_transaction(tx_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE transactions SET undone=1 WHERE id=?", (tx_id,))
    conn.commit()
    conn.close()


def clear_transactions(chat_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM transactions WHERE chat_id=?", (chat_id,))
    conn.commit()
    conn.close()


def get_transactions(chat_id, start_ts=None, end_ts=None, user_id=None, keyword=None, include_undone=False):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
    SELECT id, chat_id, user_id, username, display_name, target_name, kind,
           raw_amount, unit_amount, rate_used, fee_used, note, original_text,
           created_at, undone
    FROM transactions
    WHERE chat_id=?
    """
    params = [chat_id]

    if not include_undone:
        sql += " AND undone=0"

    if start_ts is not None:
        sql += " AND created_at>=?"
        params.append(start_ts)

    if end_ts is not None:
        sql += " AND created_at<=?"
        params.append(end_ts)

    if user_id is not None:
        sql += " AND user_id=?"
        params.append(user_id)

    if keyword:
        sql += " AND (display_name LIKE ? OR target_name LIKE ? OR username LIKE ? OR note LIKE ?)"
        kw = f"%{keyword}%"
        params.extend([kw, kw, kw, kw])

    sql += " ORDER BY created_at ASC, id ASC"

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows


# ================= ACCESS USERS =================
def set_trial_code(code):
    set_setting(-1, "trial_code", code)


def get_trial_code():
    return get_setting(-1, "trial_code", "")


def add_access_user(user_id, username="", granted_by=None, expires_at=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO access_users(user_id, username, granted_by, granted_at, expires_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(user_id) DO UPDATE SET
        username=excluded.username,
        granted_by=excluded.granted_by,
        granted_at=excluded.granted_at,
        expires_at=excluded.expires_at
    """, (user_id, username or "", granted_by, int(time.time()), expires_at))
    conn.commit()
    conn.close()


def remove_access_user(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM access_users WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()


def has_access_user(user_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT expires_at FROM access_users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
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
    WHERE expires_at IS NOT NULL AND expires_at <= ?
    """, (now_ts,))
    rows = cur.fetchall()
    conn.close()
    return rows
