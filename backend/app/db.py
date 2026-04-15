import os
from pathlib import Path
import secrets
import sqlite3
from datetime import datetime

from psycopg import connect as pg_connect
from psycopg.rows import dict_row

DB_PATH = Path(os.getenv("SQLITE_DB_PATH", Path(__file__).resolve().parent.parent / "line_app.db"))
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
IS_POSTGRES = bool(DATABASE_URL)


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def q(sql: str) -> str:
    if IS_POSTGRES:
        return sql.replace("?", "%s")
    return sql


def get_conn():
    if IS_POSTGRES:
        return pg_connect(DATABASE_URL, row_factory=dict_row)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def generate_group_token() -> str:
    return secrets.token_urlsafe(16)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    if IS_POSTGRES:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            id BIGSERIAL PRIMARY KEY,
            line_group_id TEXT NOT NULL UNIQUE,
            group_name TEXT,
            group_token TEXT UNIQUE,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS group_members (
            id BIGSERIAL PRIMARY KEY,
            group_id BIGINT NOT NULL,
            line_user_id TEXT NOT NULL,
            display_name TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            joined_at TEXT,
            last_seen_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(group_id, line_user_id),
            FOREIGN KEY(group_id) REFERENCES groups(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS group_admins (
            id BIGSERIAL PRIMARY KEY,
            group_id BIGINT NOT NULL,
            line_user_id TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(group_id, line_user_id),
            FOREIGN KEY(group_id) REFERENCES groups(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS shift_targets (
            id BIGSERIAL PRIMARY KEY,
            group_id BIGINT NOT NULL,
            target_month TEXT NOT NULL,
            line_user_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(group_id, target_month, line_user_id),
            FOREIGN KEY(group_id) REFERENCES groups(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS shift_requirements (
            id BIGSERIAL PRIMARY KEY,
            group_id BIGINT NOT NULL,
            target_month TEXT NOT NULL,
            shift_date TEXT NOT NULL,
            is_closed INTEGER NOT NULL DEFAULT 0,
            lunch_required INTEGER NOT NULL DEFAULT 0,
            dinner_required INTEGER NOT NULL DEFAULT 0,
            note TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(group_id, shift_date),
            FOREIGN KEY(group_id) REFERENCES groups(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS shift_submissions (
            id BIGSERIAL PRIMARY KEY,
            group_id BIGINT NOT NULL,
            line_user_id TEXT NOT NULL,
            target_month TEXT NOT NULL,
            note TEXT,
            submitted_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(group_id, line_user_id, target_month),
            FOREIGN KEY(group_id) REFERENCES groups(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS shift_entries (
            id BIGSERIAL PRIMARY KEY,
            submission_id BIGINT NOT NULL,
            shift_type TEXT NOT NULL,
            shift_date TEXT NOT NULL,
            mark TEXT NOT NULL,
            UNIQUE(submission_id, shift_type, shift_date),
            FOREIGN KEY(submission_id) REFERENCES shift_submissions(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_jobs (
            id BIGSERIAL PRIMARY KEY,
            group_id BIGINT NOT NULL,
            job_type TEXT NOT NULL,
            run_time TEXT NOT NULL,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(group_id) REFERENCES groups(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_logs (
            id BIGSERIAL PRIMARY KEY,
            group_id BIGINT NOT NULL,
            job_type TEXT NOT NULL,
            message_text TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            FOREIGN KEY(group_id) REFERENCES groups(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS webhook_events (
            id BIGSERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            source_type TEXT,
            source_group_id TEXT,
            source_user_id TEXT,
            raw_json TEXT NOT NULL,
            received_at TEXT NOT NULL
        )
        """)
    else:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            line_group_id TEXT NOT NULL UNIQUE,
            group_name TEXT,
            group_token TEXT UNIQUE,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS group_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            line_user_id TEXT NOT NULL,
            display_name TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            joined_at TEXT,
            last_seen_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(group_id, line_user_id),
            FOREIGN KEY(group_id) REFERENCES groups(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS group_admins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            line_user_id TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(group_id, line_user_id),
            FOREIGN KEY(group_id) REFERENCES groups(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS shift_targets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            target_month TEXT NOT NULL,
            line_user_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(group_id, target_month, line_user_id),
            FOREIGN KEY(group_id) REFERENCES groups(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS shift_requirements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            target_month TEXT NOT NULL,
            shift_date TEXT NOT NULL,
            is_closed INTEGER NOT NULL DEFAULT 0,
            lunch_required INTEGER NOT NULL DEFAULT 0,
            dinner_required INTEGER NOT NULL DEFAULT 0,
            note TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(group_id, shift_date),
            FOREIGN KEY(group_id) REFERENCES groups(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS shift_submissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            line_user_id TEXT NOT NULL,
            target_month TEXT NOT NULL,
            note TEXT,
            submitted_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(group_id, line_user_id, target_month),
            FOREIGN KEY(group_id) REFERENCES groups(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS shift_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            submission_id INTEGER NOT NULL,
            shift_type TEXT NOT NULL,
            shift_date TEXT NOT NULL,
            mark TEXT NOT NULL,
            UNIQUE(submission_id, shift_type, shift_date),
            FOREIGN KEY(submission_id) REFERENCES shift_submissions(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            job_type TEXT NOT NULL,
            run_time TEXT NOT NULL,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(group_id) REFERENCES groups(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            job_type TEXT NOT NULL,
            message_text TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            FOREIGN KEY(group_id) REFERENCES groups(id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS webhook_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            source_type TEXT,
            source_group_id TEXT,
            source_user_id TEXT,
            raw_json TEXT NOT NULL,
            received_at TEXT NOT NULL
        )
        """)

    conn.commit()

    cur.execute(q("SELECT id, group_token FROM groups"))
    rows = cur.fetchall()
    for row in rows:
        if not row["group_token"]:
            cur.execute(
                q("UPDATE groups SET group_token = ?, updated_at = ? WHERE id = ?"),
                (generate_group_token(), now_iso(), row["id"]),
            )

    conn.commit()
    conn.close()


def save_webhook_event(
    event_type: str,
    source_type: str | None,
    source_group_id: str | None,
    source_user_id: str | None,
    raw_json: str,
):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        q("""
        INSERT INTO webhook_events (
            event_type, source_type, source_group_id, source_user_id, raw_json, received_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """),
        (
            event_type,
            source_type,
            source_group_id,
            source_user_id,
            raw_json,
            now_iso(),
        ),
    )
    conn.commit()
    conn.close()


def save_group(line_group_id: str, group_name: str | None = None):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        q("SELECT id, group_token FROM groups WHERE line_group_id = ?"),
        (line_group_id,),
    )
    existing = cur.fetchone()

    if existing:
        cur.execute(
            q("""
            UPDATE groups
            SET group_name = COALESCE(?, group_name),
                updated_at = ?
            WHERE line_group_id = ?
            """),
            (group_name, now_iso(), line_group_id),
        )
    else:
        cur.execute(
            q("""
            INSERT INTO groups (
                line_group_id, group_name, group_token, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?)
            """),
            (
                line_group_id,
                group_name,
                generate_group_token(),
                now_iso(),
                now_iso(),
            ),
        )

    conn.commit()
    conn.close()


def get_group_by_line_group_id(line_group_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        q("""
        SELECT id, line_group_id, group_name, group_token, created_at, updated_at
        FROM groups
        WHERE line_group_id = ?
        """),
        (line_group_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def get_group_by_token(group_token: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        q("""
        SELECT id, line_group_id, group_name, group_token, created_at, updated_at
        FROM groups
        WHERE group_token = ?
        """),
        (group_token,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def upsert_group_member(
    line_group_id: str,
    line_user_id: str,
    display_name: str | None = None,
    active: bool = True,
):
    group = get_group_by_line_group_id(line_group_id)
    if not group:
        save_group(line_group_id)
        group = get_group_by_line_group_id(line_group_id)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        q("""
        INSERT INTO group_members (
            group_id, line_user_id, display_name, is_active,
            joined_at, last_seen_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(group_id, line_user_id)
        DO UPDATE SET
            display_name = COALESCE(excluded.display_name, group_members.display_name),
            is_active = excluded.is_active,
            last_seen_at = excluded.last_seen_at,
            updated_at = excluded.updated_at
        """),
        (
            group["id"],
            line_user_id,
            display_name,
            1 if active else 0,
            now_iso(),
            now_iso(),
            now_iso(),
            now_iso(),
        ),
    )

    conn.commit()
    conn.close()


def mark_group_member_left(line_group_id: str, line_user_id: str):
    group = get_group_by_line_group_id(line_group_id)
    if not group:
        return

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        q("""
        UPDATE group_members
        SET is_active = 0,
            updated_at = ?
        WHERE group_id = ? AND line_user_id = ?
        """),
        (now_iso(), group["id"], line_user_id),
    )
    conn.commit()
    conn.close()


def get_group_members(group_id: int, active_only: bool = True):
    conn = get_conn()
    cur = conn.cursor()

    if active_only:
        cur.execute(
            q("""
            SELECT id, group_id, line_user_id, display_name, is_active, joined_at, last_seen_at
            FROM group_members
            WHERE group_id = ? AND is_active = 1
            ORDER BY COALESCE(display_name, line_user_id)
            """),
            (group_id,),
        )
    else:
        cur.execute(
            q("""
            SELECT id, group_id, line_user_id, display_name, is_active, joined_at, last_seen_at
            FROM group_members
            WHERE group_id = ?
            ORDER BY COALESCE(display_name, line_user_id)
            """),
            (group_id,),
        )

    rows = cur.fetchall()
    conn.close()
    return rows


def save_shift_submission(
    group_id: int,
    line_user_id: str,
    target_month: str,
    lunch_days: list[int],
    dinner_days: list[int],
    note: str = "",
):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        q("""
        SELECT id
        FROM shift_submissions
        WHERE group_id = ? AND line_user_id = ? AND target_month = ?
        """),
        (group_id, line_user_id, target_month),
    )
    existing = cur.fetchone()

    if existing:
        submission_id = existing["id"]
        cur.execute(
            q("""
            UPDATE shift_submissions
            SET note = ?, updated_at = ?, submitted_at = ?
            WHERE id = ?
            """),
            (note, now_iso(), now_iso(), submission_id),
        )
        cur.execute(q("DELETE FROM shift_entries WHERE submission_id = ?"), (submission_id,))
    else:
        if IS_POSTGRES:
            cur.execute(
                q("""
                INSERT INTO shift_submissions (
                    group_id, line_user_id, target_month, note, submitted_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                RETURNING id
                """),
                (group_id, line_user_id, target_month, note, now_iso(), now_iso()),
            )
            submission_id = cur.fetchone()["id"]
        else:
            cur.execute(
                q("""
                INSERT INTO shift_submissions (
                    group_id, line_user_id, target_month, note, submitted_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """),
                (group_id, line_user_id, target_month, note, now_iso(), now_iso()),
            )
            submission_id = cur.lastrowid

    for day in sorted(set(lunch_days)):
        shift_date = f"{target_month}-{day:02d}"
        cur.execute(
            q("""
            INSERT INTO shift_entries (submission_id, shift_type, shift_date, mark)
            VALUES (?, ?, ?, ?)
            """),
            (submission_id, "lunch", shift_date, "○"),
        )

    for day in sorted(set(dinner_days)):
        shift_date = f"{target_month}-{day:02d}"
        cur.execute(
            q("""
            INSERT INTO shift_entries (submission_id, shift_type, shift_date, mark)
            VALUES (?, ?, ?, ?)
            """),
            (submission_id, "dinner", shift_date, "○"),
        )

    conn.commit()
    conn.close()


def get_shift_submission(group_id: int, line_user_id: str, target_month: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        q("""
        SELECT id, note, submitted_at, updated_at
        FROM shift_submissions
        WHERE group_id = ? AND line_user_id = ? AND target_month = ?
        """),
        (group_id, line_user_id, target_month),
    )
    submission = cur.fetchone()

    if not submission:
        conn.close()
        return None

    cur.execute(
        q("""
        SELECT shift_type, shift_date, mark
        FROM shift_entries
        WHERE submission_id = ?
        ORDER BY shift_type, shift_date
        """),
        (submission["id"],),
    )
    entries = cur.fetchall()
    conn.close()

    lunch_days = []
    dinner_days = []

    for entry in entries:
        day = int(entry["shift_date"].split("-")[-1])
        if entry["shift_type"] == "lunch":
            lunch_days.append(day)
        elif entry["shift_type"] == "dinner":
            dinner_days.append(day)

    return {
        "submission_id": submission["id"],
        "note": submission["note"] or "",
        "submitted_at": submission["submitted_at"],
        "updated_at": submission["updated_at"],
        "lunch_days": sorted(lunch_days),
        "dinner_days": sorted(dinner_days),
    }


def get_shift_board(group_id: int, target_month: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        q("""
        SELECT line_user_id, id
        FROM shift_submissions
        WHERE group_id = ? AND target_month = ?
        """),
        (group_id, target_month),
    )
    submissions = cur.fetchall()

    submission_map = {row["id"]: row["line_user_id"] for row in submissions}
    user_map = {row["line_user_id"]: {"lunch": [], "dinner": []} for row in submissions}

    if submission_map:
        placeholders = ",".join(["?"] * len(submission_map.keys()))
        cur.execute(
            q(f"""
            SELECT submission_id, shift_type, shift_date, mark
            FROM shift_entries
            WHERE submission_id IN ({placeholders})
            ORDER BY submission_id, shift_type, shift_date
            """),
            tuple(submission_map.keys()),
        )
        entries = cur.fetchall()

        for entry in entries:
            line_user_id = submission_map[entry["submission_id"]]
            day = int(entry["shift_date"].split("-")[-1])
            shift_type = entry["shift_type"]
            user_map[line_user_id][shift_type].append(day)

    conn.close()

    lunch = {}
    dinner = {}
    for line_user_id, value in user_map.items():
        lunch[line_user_id] = sorted(value["lunch"])
        dinner[line_user_id] = sorted(value["dinner"])

    return {"lunch": lunch, "dinner": dinner}


def get_submission_status(group_id: int, target_month: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        q("""
        SELECT line_user_id
        FROM shift_targets
        WHERE group_id = ? AND target_month = ?
        """),
        (group_id, target_month),
    )
    target_ids = {row["line_user_id"] for row in cur.fetchall()}

    cur.execute(
        q("""
        SELECT line_user_id
        FROM shift_submissions
        WHERE group_id = ? AND target_month = ?
        """),
        (group_id, target_month),
    )
    submitted_ids = {row["line_user_id"] for row in cur.fetchall()}

    conn.close()

    return {
        "target_ids": target_ids,
        "submitted_ids": submitted_ids,
        "missing_ids": target_ids - submitted_ids,
    }


def is_group_admin(group_id: int, line_user_id: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        q("""
        SELECT 1
        FROM group_admins
        WHERE group_id = ? AND line_user_id = ? AND is_active = 1
        LIMIT 1
        """),
        (group_id, line_user_id),
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def add_group_admin(group_id: int, line_user_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        q("""
        INSERT INTO group_admins (group_id, line_user_id, is_active, created_at, updated_at)
        VALUES (?, ?, 1, ?, ?)
        ON CONFLICT(group_id, line_user_id)
        DO UPDATE SET is_active = 1, updated_at = excluded.updated_at
        """),
        (group_id, line_user_id, now_iso(), now_iso()),
    )
    conn.commit()
    conn.close()


def get_group_admins(group_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        q("""
        SELECT id, group_id, line_user_id, is_active, created_at, updated_at
        FROM group_admins
        WHERE group_id = ? AND is_active = 1
        ORDER BY line_user_id
        """),
        (group_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def save_shift_targets(group_id: int, target_month: str, line_user_ids: list[str]):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        q("DELETE FROM shift_targets WHERE group_id = ? AND target_month = ?"),
        (group_id, target_month),
    )

    for line_user_id in sorted(set(line_user_ids)):
        cur.execute(
            q("""
            INSERT INTO shift_targets (group_id, target_month, line_user_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """),
            (group_id, target_month, line_user_id, now_iso(), now_iso()),
        )

    conn.commit()
    conn.close()


def get_shift_targets(group_id: int, target_month: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        q("""
        SELECT id, group_id, target_month, line_user_id, created_at, updated_at
        FROM shift_targets
        WHERE group_id = ? AND target_month = ?
        ORDER BY line_user_id
        """),
        (group_id, target_month),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def save_shift_requirements(group_id: int, target_month: str, requirements: list[dict]):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        q("DELETE FROM shift_requirements WHERE group_id = ? AND target_month = ?"),
        (group_id, target_month),
    )

    for req in requirements:
        cur.execute(
            q("""
            INSERT INTO shift_requirements (
                group_id, target_month, shift_date, is_closed,
                lunch_required, dinner_required, note, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """),
            (
                group_id,
                target_month,
                req["shift_date"],
                1 if req.get("is_closed", False) else 0,
                int(req.get("lunch_required", 0)),
                int(req.get("dinner_required", 0)),
                req.get("note", ""),
                now_iso(),
                now_iso(),
            ),
        )

    conn.commit()
    conn.close()


def get_shift_requirements(group_id: int, target_month: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        q("""
        SELECT id, group_id, target_month, shift_date, is_closed,
               lunch_required, dinner_required, note, created_at, updated_at
        FROM shift_requirements
        WHERE group_id = ? AND target_month = ?
        ORDER BY shift_date
        """),
        (group_id, target_month),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def update_group_name(line_group_id: str, group_name: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        q("""
        UPDATE groups
        SET group_name = ?, updated_at = ?
        WHERE line_group_id = ?
        """),
        (group_name, now_iso(), line_group_id),
    )
    conn.commit()
    conn.close()


def upsert_group_member_with_name(
    group_id: int,
    line_user_id: str,
    display_name: str | None = None,
    active: bool = True,
):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        q("""
        INSERT INTO group_members (
            group_id, line_user_id, display_name, is_active,
            joined_at, last_seen_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(group_id, line_user_id)
        DO UPDATE SET
            display_name = COALESCE(excluded.display_name, group_members.display_name),
            is_active = excluded.is_active,
            last_seen_at = excluded.last_seen_at,
            updated_at = excluded.updated_at
        """),
        (
            group_id,
            line_user_id,
            display_name,
            1 if active else 0,
            now_iso(),
            now_iso(),
            now_iso(),
            now_iso(),
        ),
    )

    conn.commit()
    conn.close()


def save_bot_log(group_id: int, job_type: str, message_text: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        q("""
        INSERT INTO bot_logs (group_id, job_type, message_text, sent_at)
        VALUES (?, ?, ?, ?)
        """),
        (group_id, job_type, message_text, now_iso()),
    )
    conn.commit()
    conn.close()


def create_group_if_not_exists(line_group_id: str, group_name: str | None = None):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        q("SELECT id, line_group_id, group_name, group_token, created_at, updated_at FROM groups WHERE line_group_id = ?"),
        (line_group_id,),
    )
    row = cur.fetchone()

    if row:
        if group_name:
            cur.execute(
                q("""
                UPDATE groups
                SET group_name = ?, updated_at = ?
                WHERE line_group_id = ?
                """),
                (group_name, now_iso(), line_group_id),
            )
            conn.commit()
            cur.execute(
                q("SELECT id, line_group_id, group_name, group_token, created_at, updated_at FROM groups WHERE line_group_id = ?"),
                (line_group_id,),
            )
            row = cur.fetchone()

        conn.close()
        return row

    group_token = secrets.token_urlsafe(16)

    cur.execute(
        q("""
        INSERT INTO groups (
            line_group_id, group_name, group_token, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?)
        """),
        (
            line_group_id,
            group_name,
            group_token,
            now_iso(),
            now_iso(),
        ),
    )
    conn.commit()

    cur.execute(
        q("SELECT id, line_group_id, group_name, group_token, created_at, updated_at FROM groups WHERE line_group_id = ?"),
        (line_group_id,),
    )
    row = cur.fetchone()

    conn.close()
    return row