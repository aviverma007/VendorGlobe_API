"""
PR -> PO journey endpoint.

Serves the DASHBOARD_SWD "PR to PO" page with one JSON payload that
carries all three legs of the journey for a date window:

  sap_pr : PRD_PR line rows from SWDBIDB on the SAP BI server
           (PR created Erdat -> released Frgdt -> follow-on PO Ebeln)
  sap_po : PRD_PurchaseOrder header rows for the POs those PRs created
           (created BADAT, release state FRGZU/FRGKE/PROCSTAT)
  vg     : VendorGlobe QMS PR + NFA rows from our local synced table
           (per-level approval dates, pending-with, statuses)

The dashboard stitches them client-side by PR number
(PRD_PR.Banfn == VendorGlobe EPR_No) and PO number
(PRD_PR.Ebeln == PRD_PurchaseOrder.EBELN).

Config (env overrides, VG_ prefix, see db_config):
  SAP_DB_SERVER (192.168.66.33), SAP_DB_NAME (SWDBIDB),
  SAP_DB_USER / SAP_DB_PASSWORD -- leave empty for Windows trusted auth.
"""

import os
from datetime import date, timedelta

from flask import jsonify, request

import db_config as cfg


def _env(name, default):
    return os.environ.get("VG_" + name, default)


SAP_DB_SERVER = _env("SAP_DB_SERVER", "192.168.66.33")
SAP_DB_NAME = _env("SAP_DB_NAME", "SWDBIDB")
SAP_DB_USER = _env("SAP_DB_USER", "")
SAP_DB_PASSWORD = _env("SAP_DB_PASSWORD", "")


def _sap_connection_string():
    base = (
        f"DRIVER={{{cfg.ODBC_DRIVER}}};SERVER={SAP_DB_SERVER};"
        f"DATABASE={SAP_DB_NAME};TrustServerCertificate=yes;"
    )
    if SAP_DB_USER:
        return base + f"UID={SAP_DB_USER};PWD={SAP_DB_PASSWORD};"
    return base + "Trusted_Connection=yes;"


# Slim column sets -- keep the payload lean; the page computes the rest.
SAP_PR_COLS = [
    "Banfn", "Bnfpo", "Erdat", "Badat", "Frgdt", "RelStatus", "Frgkz",
    "Statu", "Loekz", "Ebeln", "Bedat", "Ernam", "Afnam", "Ekgrp",
    "Eknam", "Ekorg", "Werks", "PlantDesc", "Bsart", "Txz01", "Netwr",
]
SAP_PO_COLS = [
    "EBELN", "BADAT", "AEDAT", "FRGZU", "FRGKE", "PROCSTAT", "LOEKZ",
    "NAME1", "BSART", "EKGRP", "EKNAM", "PLANT_DESC", "NETWR",
    "NETWR_INV", "TXZ01",
]
VG_COLS = [
    "EPR_No", "Is_Sap_Pr", "Project_Name", "PRH_Category_Name",
    "PRH_Sub_Category_Name", "Scope", "PR_Budget",
    "PR_Created_Date", "PRN_Date", "PRH_Status", "PRH_Status_Desc",
    "PR_Pending_With", "PR_Pending_Since",
    "Validator_One", "Validator_One_Date", "Validator_Two", "Validator_Two_Date",
    "CP_Team", "CP_Team_Date", "PR_Assigners", "Assignee_Team_Date",
    "NFA_No", "ENFA_No", "NFA_Created_Date", "Submitted_Date", "ENFA_Date",
    "nfa_status", "NFA_Status_Desc", "NFA_Pending_With", "NFA_Pending_Since",
    "Level_One_Team", "Level_One_Date", "Level_Two_Team", "Level_Two_Date",
    "Level_Three_Team", "Level_Three_Date", "Level_Four_Team", "Level_Four_Date",
    "Level_Five_Team", "Level_Five_Date", "Level_Six_Team", "Level_Six_Date",
    "Level_Seven_Team", "Level_Seven_Date", "Level_Eight_Team", "Level_Eight_Date",
    "Vendor_Name", "Amount_Including_Tax", "Amount_Excluding_Tax",
    "PR_To_NFA_TAT",
]


def _rows(cur, cols):
    got = [d[0] for d in cur.description]
    return [
        {c: (str(v).strip() if v is not None else None) for c, v in zip(got, r)}
        for r in cur.fetchall()
    ]


def _query_sap(startdate, enddate):
    import pyodbc
    conn = pyodbc.connect(_sap_connection_string(), timeout=15)
    try:
        cur = conn.cursor()
        col_list = ", ".join(f"[{c}]" for c in SAP_PR_COLS)
        cur.execute(
            f"SELECT {col_list} FROM [dbo].[PRD_PR] "
            f"WHERE [Erdat] >= ? AND [Erdat] <= ? ORDER BY [Erdat] DESC",
            startdate, enddate,
        )
        sap_pr = _rows(cur, SAP_PR_COLS)

        po_cols = ", ".join(
            f"MAX([{c}]) AS [{c}]" if c not in ("EBELN", "NETWR", "NETWR_INV")
            else (f"[{c}]" if c == "EBELN" else f"SUM([{c}]) AS [{c}]")
            for c in SAP_PO_COLS
        )
        cur.execute(
            f"SELECT {po_cols} FROM [dbo].[PRD_PurchaseOrder] "
            f"WHERE [EBELN] IN (SELECT DISTINCT [Ebeln] FROM [dbo].[PRD_PR] "
            f"  WHERE [Erdat] >= ? AND [Erdat] <= ? AND [Ebeln] IS NOT NULL AND [Ebeln] <> '') "
            f"GROUP BY [EBELN]",
            startdate, enddate,
        )
        sap_po = _rows(cur, SAP_PO_COLS)
        return sap_pr, sap_po
    finally:
        conn.close()


def _query_vg(startdate, enddate, epr_set):
    """VendorGlobe rows from our local synced table: created in the window
    OR matching a SAP PR from the window (covers replication lag)."""
    import pyodbc
    conn = pyodbc.connect(
        f"DRIVER={{{cfg.ODBC_DRIVER}}};SERVER={cfg.DB_SERVER};"
        f"DATABASE={cfg.DB_NAME};Trusted_Connection=yes;TrustServerCertificate=yes;",
        timeout=8,
    )
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM [dbo].[{cfg.NFATAT_TABLE_NAME}]")
        got = [d[0] for d in cur.description]
        out = []
        lo, hi = startdate, enddate + " 23:59:59"
        for r in cur.fetchall():
            d = dict(zip(got, r))
            epr = str(d.get("EPR_No") or "").strip()
            created = str(d.get("PR_Created_Date") or "")
            if (lo <= created <= hi) or (epr in epr_set):
                out.append({c: (str(d[c]).strip() if d.get(c) is not None else None)
                            for c in VG_COLS if c in d})
        return out
    finally:
        conn.close()


def register(app):
    @app.after_request
    def _pr2po_cors(resp):
        # The DASHBOARD_SWD SPA (port 3000) fetches these endpoints
        # cross-origin; data is read-only and already on the intranet.
        if request.path.startswith("/pr2po"):
            resp.headers["Access-Control-Allow-Origin"] = "*"
            resp.headers["Access-Control-Allow-Headers"] = "*"
        return resp

    @app.route("/pr2po/health")
    def pr2po_health():
        status = {"vg_db": None, "sap_db": None}
        try:
            import pyodbc
            c = pyodbc.connect(_sap_connection_string(), timeout=5)
            c.close()
            status["sap_db"] = "ok"
        except Exception as e:  # noqa: BLE001
            status["sap_db"] = f"error: {e}"
        try:
            import pyodbc
            c = pyodbc.connect(
                f"DRIVER={{{cfg.ODBC_DRIVER}}};SERVER={cfg.DB_SERVER};"
                f"DATABASE={cfg.DB_NAME};Trusted_Connection=yes;TrustServerCertificate=yes;",
                timeout=5,
            )
            c.close()
            status["vg_db"] = "ok"
        except Exception as e:  # noqa: BLE001
            status["vg_db"] = f"error: {e}"
        ok = status["vg_db"] == "ok" and status["sap_db"] == "ok"
        return jsonify({"ok": ok, **status})

    @app.route("/pr2po/data")
    def pr2po_data():
        try:
            enddate = request.args.get("enddate") or date.today().isoformat()
            startdate = request.args.get("startdate") or (
                date.today() - timedelta(days=90)
            ).isoformat()

            sap_error = None
            sap_pr, sap_po = [], []
            try:
                sap_pr, sap_po = _query_sap(startdate, enddate)
            except Exception as e:  # noqa: BLE001
                sap_error = str(e)

            epr_set = {r["Banfn"] for r in sap_pr if r.get("Banfn")}
            vg = _query_vg(startdate, enddate, epr_set)

            return jsonify({
                "ok": True,
                "meta": {
                    "startdate": startdate, "enddate": enddate,
                    "sap_pr_lines": len(sap_pr), "sap_po": len(sap_po),
                    "vg_rows": len(vg), "sap_error": sap_error,
                },
                "sap_pr": sap_pr, "sap_po": sap_po, "vg": vg,
            })
        except Exception as e:  # noqa: BLE001
            return jsonify({"ok": False, "error": str(e)}), 200
