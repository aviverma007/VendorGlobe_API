"""
PR -> PO journey endpoints for the DASHBOARD_SWD "PR to PO" page.

Everything is served from LOCAL databases on this server — no request
ever waits on the SAP BI server or the vendor:

  sap_pr : PR2PO.dbo.SAP_PR   (mirror of SWDBIDB.dbo.PRD_PR,
           synced every 5 min by sap_sync.py)
  sap_po : PR2PO.dbo.SAP_PO   (mirror of SWDBIDB.dbo.PRD_PurchaseOrder),
           aggregated to PO-header level
  vg     : VendorGlobe_PR.dbo.PRNFATatReportHistory (existing 5-min
           VendorGlobe sync — QMS PR + NFA approval levels)

The dashboard stitches the legs client-side by PR number
(SAP_PR.Banfn == VendorGlobe EPR_No) and PO number
(SAP_PR.Ebeln == SAP_PO.EBELN).

Endpoints:
  GET /pr2po/data?startdate=YYYY-MM-DD&enddate=YYYY-MM-DD
  GET /pr2po/health   (local DBs + source .33 + sync freshness)
"""

import os
from datetime import date, timedelta

from flask import jsonify, request

import db_config as cfg
import sap_sync


def _env(name, default):
    return os.environ.get("VG_" + name, default)


PR2PO_DB_NAME = _env("PR2PO_DB_NAME", "PR2PO")

# Slim column sets -- keep the payload lean; the page computes the rest.
SAP_PR_COLS = [
    "Banfn", "Bnfpo", "Erdat", "Badat", "Frgdt", "RelStatus", "Frgkz",
    "Statu", "Loekz", "Ebeln", "Bedat", "Ernam", "Afnam", "Ekgrp",
    "Eknam", "Ekorg", "Werks", "PlantDesc", "Bsart", "Txz01", "Netwr",
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


def _connect(database):
    import pyodbc
    return pyodbc.connect(
        f"DRIVER={{{cfg.ODBC_DRIVER}}};SERVER={cfg.DB_SERVER};"
        f"DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes;",
        timeout=8,
    )


def _dict_rows(cur):
    got = [d[0] for d in cur.description]
    return [
        {c: (str(v).strip() if v is not None else None) for c, v in zip(got, r)}
        for r in cur.fetchall()
    ]


def _query_sap_local(startdate, enddate, extra_prs):
    """SAP PR lines created in the window OR whose PR number appears in
    the VendorGlobe window (two-way match: a QMS PR replicated from a
    SAP PR created before the window still needs its SAP leg + PO link).
    Then PO headers for every follow-on PO of that PR set."""
    conn = _connect(PR2PO_DB_NAME)
    try:
        cur = conn.cursor()
        extra = sorted({p for p in extra_prs if p})
        col_list = ", ".join(f"[{c}]" for c in SAP_PR_COLS)

        cur.execute("IF OBJECT_ID('tempdb..#prs') IS NOT NULL DROP TABLE #prs; "
                    "CREATE TABLE #prs (p varchar(20) PRIMARY KEY);")
        for i in range(0, len(extra), 500):
            chunk = extra[i:i + 500]
            cur.execute("INSERT INTO #prs (p) VALUES " + ",".join(["(?)"] * len(chunk)), chunk)

        cur.execute(
            f"SELECT {col_list} FROM [dbo].[SAP_PR] "
            f"WHERE ([Erdat] >= ? AND [Erdat] <= ?) "
            f"   OR [Banfn] IN (SELECT p FROM #prs) "
            f"ORDER BY [Erdat] DESC",
            startdate, enddate,
        )
        sap_pr = _dict_rows(cur)

        # PO headers for the POs those PR lines created (line -> header agg).
        cur.execute(
            "SELECT [EBELN], MIN([BADAT]) AS [BADAT], MAX([AEDAT]) AS [AEDAT], "
            "  MAX([FRGZU]) AS [FRGZU], MAX([FRGKE]) AS [FRGKE], "
            "  MAX([PROCSTAT]) AS [PROCSTAT], MAX([LOEKZ]) AS [LOEKZ], "
            "  MAX([NAME1]) AS [NAME1], MAX([BSART]) AS [BSART], "
            "  MAX([EKGRP]) AS [EKGRP], MAX([EKNAM]) AS [EKNAM], "
            "  MAX([PLANT_DESC]) AS [PLANT_DESC], MAX([TXZ01]) AS [TXZ01], "
            "  SUM([NETWR]) AS [NETWR], SUM([NETWR_INV]) AS [NETWR_INV] "
            "FROM [dbo].[SAP_PO] "
            "WHERE [EBELN] IN (SELECT DISTINCT [Ebeln] FROM [dbo].[SAP_PR] "
            "  WHERE (([Erdat] >= ? AND [Erdat] <= ?) OR [Banfn] IN (SELECT p FROM #prs)) "
            "  AND [Ebeln] IS NOT NULL AND [Ebeln] <> '') "
            "GROUP BY [EBELN]",
            startdate, enddate,
        )
        sap_po = _dict_rows(cur)
        return sap_pr, sap_po
    finally:
        conn.close()


def _query_vg(startdate, enddate, epr_set):
    """VendorGlobe rows from the existing synced table: created in the
    window OR matching a SAP PR from the window (covers replication lag,
    and VendorGlobe-only PRs that never came from SAP)."""
    conn = _connect(cfg.DB_NAME)
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


def _sync_freshness():
    """Age of the newest fetched_at per mirror table, in seconds."""
    out = {}
    try:
        conn = _connect(PR2PO_DB_NAME)
        try:
            cur = conn.cursor()
            for t in ("SAP_PR", "SAP_PO"):
                cur.execute(
                    f"SELECT DATEDIFF(second, MAX(fetched_at), SYSDATETIME()), COUNT(*) "
                    f"FROM [dbo].[{t}]"
                )
                age, n = cur.fetchone()
                out[t] = {"rows": n, "last_sync_age_s": age}
        finally:
            conn.close()
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)
    return out


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
        import pyodbc
        status = {"pr2po_db": None, "vg_db": None, "sap_source": None,
                  "sync": _sync_freshness()}
        for key, db in (("pr2po_db", PR2PO_DB_NAME), ("vg_db", cfg.DB_NAME)):
            try:
                _connect(db).close()
                status[key] = "ok"
            except Exception as e:  # noqa: BLE001
                status[key] = f"error: {e}"
        try:
            c = pyodbc.connect(sap_sync._sap_connection_string(), timeout=5)
            c.close()
            status["sap_source"] = "ok"
        except Exception as e:  # noqa: BLE001
            status["sap_source"] = f"error: {e}"
        ok = status["pr2po_db"] == "ok" and status["vg_db"] == "ok"
        return jsonify({"ok": ok, **status})

    @app.route("/pr2po/data")
    def pr2po_data():
        try:
            enddate = request.args.get("enddate") or date.today().isoformat()
            startdate = request.args.get("startdate") or (
                date.today() - timedelta(days=90)
            ).isoformat()

            # VendorGlobe first: its window EPRs widen the SAP fetch
            # (two-way match), then the SAP PR set widens VG in return.
            vg_window = _query_vg(startdate, enddate, set())
            vg_eprs = {str(r.get("EPR_No") or "") for r in vg_window}

            sap_error = None
            sap_pr, sap_po = [], []
            try:
                sap_pr, sap_po = _query_sap_local(startdate, enddate, vg_eprs)
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
                    "sync": _sync_freshness(),
                },
                "sap_pr": sap_pr, "sap_po": sap_po, "vg": vg,
            })
        except Exception as e:  # noqa: BLE001
            return jsonify({"ok": False, "error": str(e)}), 200
