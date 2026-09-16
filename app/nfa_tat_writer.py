"""
Upsert writer for the PR/NFA TAT report -> SQL Server.

One row per EPR_No in dbo.PRNFATatReportHistory, same upsert pattern
as db_writer.py: insert new PRs, update changed ones, skip identical.
Pulls a rolling window (today - NFATAT_ROLLING_DAYS to today) each
cycle so recently-changed PRs are always covered.
"""

import re
import time
import threading
import os
import sys
from datetime import datetime, timedelta

import requests
import pyodbc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db_config as cfg

_lock = threading.Lock()
_known_columns = set()

KEY_COLUMN = cfg.NFATAT_PR_COLUMN  # "EPR_No"
META_COLUMNS = {"id", "fetched_at", "first_seen", KEY_COLUMN}


def _sanitize_column_name(raw_name):
    name = re.sub(r"[^0-9A-Za-z_]", "_", str(raw_name)).strip("_")
    if not name:
        name = "col"
    if name[0].isdigit():
        name = "c_" + name
    return name[:120]


def _db_connection_string():
    return (
        f"DRIVER={{{cfg.ODBC_DRIVER}}};"
        f"SERVER={cfg.DB_SERVER};"
        f"DATABASE={cfg.DB_NAME};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
    )


def _master_connection_string():
    return (
        f"DRIVER={{{cfg.ODBC_DRIVER}}};"
        f"SERVER={cfg.DB_SERVER};"
        f"DATABASE=master;"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
    )


def ensure_database_exists():
    conn = pyodbc.connect(_master_connection_string(), autocommit=True)
    try:
        cur = conn.cursor()
        cur.execute(
            "IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = ?) "
            "EXEC('CREATE DATABASE [' + ? + ']')",
            cfg.DB_NAME, cfg.DB_NAME,
        )
    finally:
        conn.close()


def ensure_table_exists(conn):
    cur = conn.cursor()
    cur.execute(
        "IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = ?) "
        "EXEC('CREATE TABLE [dbo].[' + ? + '] ("
        "[" + KEY_COLUMN + "] NVARCHAR(100) NOT NULL PRIMARY KEY, "
        "fetched_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(), "
        "first_seen DATETIME2 NOT NULL DEFAULT SYSDATETIME())')",
        cfg.NFATAT_TABLE_NAME, cfg.NFATAT_TABLE_NAME,
    )
    conn.commit()

    cur.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?",
        cfg.NFATAT_TABLE_NAME,
    )
    global _known_columns
    _known_columns = {row[0] for row in cur.fetchall()}

    if "first_seen" not in _known_columns:
        cur.execute(
            f"ALTER TABLE [dbo].[{cfg.NFATAT_TABLE_NAME}] "
            f"ADD first_seen DATETIME2 NOT NULL DEFAULT SYSDATETIME()"
        )
        conn.commit()
        _known_columns.add("first_seen")


def ensure_columns(conn, column_names):
    global _known_columns
    missing = [c for c in column_names if c not in _known_columns]
    if not missing:
        return
    cur = conn.cursor()
    for col in missing:
        cur.execute(
            f"ALTER TABLE [dbo].[{cfg.NFATAT_TABLE_NAME}] ADD [{col}] NVARCHAR(MAX) NULL"
        )
    conn.commit()
    _known_columns.update(missing)


def normalize_rows(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "rows", "result", "results", "d"):
            if key in data and isinstance(data[key], list):
                return data[key]
        return [data]
    return []


def _current_window():
    end = datetime.now().date()
    start = end - timedelta(days=cfg.NFATAT_ROLLING_DAYS)
    return start.isoformat(), end.isoformat()


def _load_existing(conn, data_columns):
    cur = conn.cursor()
    cols = [KEY_COLUMN] + list(data_columns)
    col_list = ", ".join(f"[{c}]" for c in cols)
    cur.execute(f"SELECT {col_list} FROM [dbo].[{cfg.NFATAT_TABLE_NAME}]")
    existing = {}
    for rec in cur.fetchall():
        key = rec[0]
        existing[str(key)] = {c: rec[i + 1] for i, c in enumerate(data_columns)}
    return existing


def fetch_and_store():
    startdate, enddate = _current_window()
    url = f"{cfg.NFATAT_SOURCE_URL_BASE}?startdate={startdate}&enddate={enddate}"

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    rows = normalize_rows(resp.json())
    if not rows:
        return (0, 0, 0)

    sanitized_rows = []
    all_columns = set()
    for row in rows:
        sanitized = {_sanitize_column_name(k): v for k, v in row.items()}
        sanitized_rows.append(sanitized)
        all_columns.update(sanitized.keys())

    # The source report can contain the same EPR_No on multiple rows
    # (line items / approval steps). Keep only the LAST occurrence per
    # key so the upsert has one row per EPR_No.
    deduped = {}
    for row in sanitized_rows:
        key = row.get(KEY_COLUMN)
        if key is not None:
            deduped[str(key)] = row
    sanitized_rows = list(deduped.values())

    data_columns = sorted(all_columns - META_COLUMNS)
    now = datetime.now()
    inserted = updated = unchanged = 0

    with _lock:
        conn = pyodbc.connect(_db_connection_string(), autocommit=False)
        try:
            ensure_columns(conn, all_columns)
            existing = _load_existing(conn, data_columns)
            cur = conn.cursor()

            for row in sanitized_rows:
                key = row.get(KEY_COLUMN)
                if key is None:
                    continue
                key = str(key)
                new_vals = {
                    c: (None if row.get(c) is None else str(row.get(c)))
                    for c in data_columns
                }

                if key not in existing:
                    cols = [KEY_COLUMN, "fetched_at", "first_seen"] + data_columns
                    placeholders = ", ".join(["?"] * len(cols))
                    col_list = ", ".join(f"[{c}]" for c in cols)
                    values = [key, now, now] + [new_vals[c] for c in data_columns]
                    cur.execute(
                        f"INSERT INTO [dbo].[{cfg.NFATAT_TABLE_NAME}] ({col_list}) "
                        f"VALUES ({placeholders})",
                        values,
                    )
                    inserted += 1
                else:
                    old_vals = {
                        c: (None if existing[key][c] is None else str(existing[key][c]))
                        for c in data_columns
                    }
                    if old_vals != new_vals:
                        set_list = ", ".join(f"[{c}] = ?" for c in data_columns)
                        values = [new_vals[c] for c in data_columns] + [now, key]
                        cur.execute(
                            f"UPDATE [dbo].[{cfg.NFATAT_TABLE_NAME}] "
                            f"SET {set_list}, fetched_at = ? "
                            f"WHERE [{KEY_COLUMN}] = ?",
                            values,
                        )
                        updated += 1
                    else:
                        unchanged += 1

            conn.commit()
        finally:
            conn.close()

    return (inserted, updated, unchanged)


def init():
    ensure_database_exists()
    conn = pyodbc.connect(_db_connection_string(), autocommit=False)
    try:
        ensure_table_exists(conn)
    finally:
        conn.close()


def run_forever(interval_seconds=None):
    interval = interval_seconds or cfg.NFATAT_WRITE_INTERVAL_SECONDS
    init()
    print(f"[nfa_tat_writer] Upserting to {cfg.DB_SERVER}/{cfg.DB_NAME}.dbo.{cfg.NFATAT_TABLE_NAME} "
          f"every {interval}s (one row per {KEY_COLUMN}, rolling {cfg.NFATAT_ROLLING_DAYS}-day window)")
    while True:
        try:
            ins, upd, same = fetch_and_store()
            if ins or upd:
                startdate, enddate = _current_window()
                print(f"[nfa_tat_writer] {datetime.now().strftime('%H:%M:%S')} "
                      f"+{ins} new, ~{upd} updated, {same} unchanged ({startdate} to {enddate})")
        except Exception as e:
            print(f"[nfa_tat_writer] ERROR: {e}")
        time.sleep(interval)


def start_background_thread():
    t = threading.Thread(target=run_forever, daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    run_forever()
