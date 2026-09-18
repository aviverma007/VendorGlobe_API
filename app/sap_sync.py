"""
SAP mirror sync: SWDBIDB (192.168.66.33) -> local PR2PO database.

Copies the two SAP BI tables that carry the SAP ends of the PR->PO
journey into a local database on this server, so the /pr2po endpoints
never query .33 at request time (fast, and resilient to .33 downtime):

    SWDBIDB.dbo.PRD_PR            -> PR2PO.dbo.SAP_PR   (key Banfn+Bnfpo)
    SWDBIDB.dbo.PRD_PurchaseOrder -> PR2PO.dbo.SAP_PO   (key EBELN+EBELP)

Same upsert-with-change-detection design as nfa_tat_writer: each cycle
reads the full source tables (~35k rows total, LAN), inserts new keys,
updates changed rows, skips identical ones. Rows deleted at source are
kept locally (marked by a stale fetched_at) - history is cheap and the
dashboard filters by date anyway.

Auth to .33 uses a read-only SQL login from env (never in the repo):
    VG_SAP_DB_USER / VG_SAP_DB_PASSWORD
Local PR2PO uses Windows trusted auth like the other writers.
"""

import os
import sys
import time
import threading
from datetime import datetime

import pyodbc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db_config as cfg

_lock = threading.Lock()


def _env(name, default):
    return os.environ.get("VG_" + name, default)


SAP_DB_SERVER = _env("SAP_DB_SERVER", "192.168.66.33")
SAP_DB_NAME = _env("SAP_DB_NAME", "SWDBIDB")
SAP_DB_USER = _env("SAP_DB_USER", "")
SAP_DB_PASSWORD = _env("SAP_DB_PASSWORD", "")
PR2PO_DB_NAME = _env("PR2PO_DB_NAME", "PR2PO")
SAP_SYNC_INTERVAL_SECONDS = int(_env("SAP_SYNC_INTERVAL_SECONDS", "300"))

# [local column, source column, SQL type] -- typed mirrors, not NVARCHAR(MAX):
# the source schema is known and stable (SAP BI extract).
PR_COLS = [
    ("Banfn", "varchar(20)"), ("Bnfpo", "varchar(10)"),
    ("Erdat", "date"), ("Badat", "date"), ("Frgdt", "date"), ("Bedat", "date"),
    ("RelStatus", "varchar(20)"), ("Frgkz", "char(1)"), ("Frgst", "varchar(5)"),
    ("Frggr", "varchar(5)"), ("Statu", "char(1)"), ("Loekz", "bit"),
    ("Procstat", "varchar(255)"), ("Ebeln", "varchar(20)"),
    ("Ernam", "varchar(50)"), ("Afnam", "varchar(50)"),
    ("Ekgrp", "varchar(20)"), ("Eknam", "varchar(255)"), ("Ekorg", "varchar(20)"),
    ("Werks", "varchar(20)"), ("PlantDesc", "varchar(255)"),
    ("Bsart", "varchar(10)"), ("Txz01", "varchar(255)"),
    ("Matkl", "varchar(20)"), ("Menge", "decimal(18,3)"),
    ("Netwr", "decimal(18,2)"), ("Bednr", "varchar(20)"),
    ("Monat", "varchar(2)"), ("Gjahr", "varchar(4)"),
]
PO_COLS = [
    ("EBELN", "varchar(20)"), ("EBELP", "int"),
    ("BADAT", "date"), ("AEDAT", "date"), ("KDATB", "date"), ("KDATE", "date"),
    ("FRGZU", "varchar(10)"), ("FRGKE", "varchar(10)"), ("PROCSTAT", "varchar(20)"),
    ("LOEKZ", "char(1)"), ("NAME1", "varchar(100)"), ("BSART", "varchar(10)"),
    ("EKGRP", "varchar(10)"), ("EKNAM", "varchar(100)"), ("EKORG", "varchar(10)"),
    ("WERKS", "varchar(10)"), ("PLANT_DESC", "varchar(100)"),
    ("MATKL", "varchar(50)"), ("TXZ01", "varchar(255)"),
    ("MENGE", "decimal(18,3)"), ("NETPR", "decimal(18,2)"),
    ("NETWR", "decimal(18,2)"), ("MENGE_DEL", "decimal(18,3)"),
    ("MENGE_INV", "decimal(18,3)"), ("NETWR_INV", "decimal(18,2)"),
    ("WAERS", "varchar(5)"), ("MONAT", "int"), ("GJAHR", "int"),
]

TABLES = [
    # (local table, source table, key columns, all columns)
    ("SAP_PR", "PRD_PR", ["Banfn", "Bnfpo"], PR_COLS),
    ("SAP_PO", "PRD_PurchaseOrder", ["EBELN", "EBELP"], PO_COLS),
]


def _sap_connection_string():
    base = (
        f"DRIVER={{{cfg.ODBC_DRIVER}}};SERVER={SAP_DB_SERVER};"
        f"DATABASE={SAP_DB_NAME};TrustServerCertificate=yes;"
    )
    if SAP_DB_USER:
        return base + f"UID={SAP_DB_USER};PWD={SAP_DB_PASSWORD};"
    return base + "Trusted_Connection=yes;"


def _local_connection_string(database=None):
    return (
        f"DRIVER={{{cfg.ODBC_DRIVER}}};SERVER={cfg.DB_SERVER};"
        f"DATABASE={database or PR2PO_DB_NAME};"
        f"Trusted_Connection=yes;TrustServerCertificate=yes;"
    )


def ensure_database_exists():
    conn = pyodbc.connect(_local_connection_string("master"), autocommit=True)
    try:
        cur = conn.cursor()
        cur.execute(
            "IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = ?) "
            "EXEC('CREATE DATABASE [' + ? + ']')",
            PR2PO_DB_NAME, PR2PO_DB_NAME,
        )
    finally:
        conn.close()


def ensure_tables_exist():
    conn = pyodbc.connect(_local_connection_string(), autocommit=True)
    try:
        cur = conn.cursor()
        for local, _src, keys, cols in TABLES:
            col_defs = ", ".join(
                f"[{c}] {t} {'NOT NULL' if c in keys else 'NULL'}"
                for c, t in cols
            )
            pk = ", ".join(f"[{k}]" for k in keys)
            cur.execute(
                "IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = ?) "
                f"EXEC('CREATE TABLE [dbo].[{local}] ({col_defs}, "
                "fetched_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(), "
                "first_seen DATETIME2 NOT NULL DEFAULT SYSDATETIME(), "
                f"CONSTRAINT [PK_{local}] PRIMARY KEY ({pk}))')",
                local,
            )
    finally:
        conn.close()


def _norm(v):
    """Comparable form of a value (source and local types can differ subtly)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, float):
        return f"{v:.4f}"
    try:
        import decimal
        if isinstance(v, decimal.Decimal):
            return f"{float(v):.4f}"
    except Exception:  # noqa: BLE001
        pass
    return str(v).strip()


def _sync_table(sap_cur, local_conn, local, src, keys, cols):
    names = [c for c, _t in cols]
    col_list = ", ".join(f"[{c}]" for c in names)
    sap_cur.execute(f"SELECT {col_list} FROM [dbo].[{src}]")
    src_rows = {}
    for r in sap_cur.fetchall():
        d = dict(zip(names, r))
        key = tuple(_norm(d[k]) for k in keys)
        if all(k is not None for k in key):
            src_rows[key] = d

    cur = local_conn.cursor()
    cur.execute(f"SELECT {col_list} FROM [dbo].[{local}]")
    existing = {}
    for r in cur.fetchall():
        d = dict(zip(names, r))
        existing[tuple(_norm(d[k]) for k in keys)] = {c: _norm(d[c]) for c in names}

    now = datetime.now()
    data_cols = [c for c in names if c not in keys]
    inserted = updated = unchanged = 0
    for key, d in src_rows.items():
        if key not in existing:
            ph = ", ".join(["?"] * (len(names) + 2))
            cur.execute(
                f"INSERT INTO [dbo].[{local}] ({col_list}, fetched_at, first_seen) "
                f"VALUES ({ph})",
                [d[c] for c in names] + [now, now],
            )
            inserted += 1
        else:
            new_vals = {c: _norm(d[c]) for c in names}
            if new_vals != existing[key]:
                set_list = ", ".join(f"[{c}] = ?" for c in data_cols)
                where = " AND ".join(f"[{k}] = ?" for k in keys)
                cur.execute(
                    f"UPDATE [dbo].[{local}] SET {set_list}, fetched_at = ? "
                    f"WHERE {where}",
                    [d[c] for c in data_cols] + [now] + list(key),
                )
                updated += 1
            else:
                unchanged += 1
    local_conn.commit()
    return inserted, updated, unchanged, len(src_rows)


def sync_once():
    with _lock:
        sap_conn = pyodbc.connect(_sap_connection_string(), timeout=20)
        local_conn = pyodbc.connect(_local_connection_string(), autocommit=False)
        try:
            sap_cur = sap_conn.cursor()
            results = {}
            for local, src, keys, cols in TABLES:
                results[local] = _sync_table(sap_cur, local_conn, local, src, keys, cols)
            return results
        finally:
            sap_conn.close()
            local_conn.close()


def init():
    ensure_database_exists()
    ensure_tables_exist()


def run_forever(interval_seconds=None):
    interval = interval_seconds or SAP_SYNC_INTERVAL_SECONDS
    init()
    auth = f"SQL login '{SAP_DB_USER}'" if SAP_DB_USER else "Windows trusted auth"
    print(f"[sap_sync] Mirroring {SAP_DB_SERVER}/{SAP_DB_NAME} (PRD_PR, PRD_PurchaseOrder) "
          f"-> {cfg.DB_SERVER}/{PR2PO_DB_NAME} every {interval}s via {auth}")
    while True:
        try:
            results = sync_once()
            changed = {t: r for t, r in results.items() if r[0] or r[1]}
            if changed:
                msg = " · ".join(
                    f"{t}: +{r[0]} new, ~{r[1]} updated of {r[3]}"
                    for t, r in changed.items()
                )
                print(f"[sap_sync] {datetime.now().strftime('%H:%M:%S')} {msg}")
        except Exception as e:  # noqa: BLE001
            print(f"[sap_sync] ERROR: {e}")
        time.sleep(interval)


def start_background_thread():
    t = threading.Thread(target=run_forever, daemon=True)
    t.start()
    return t


if __name__ == "__main__":
    run_forever()
