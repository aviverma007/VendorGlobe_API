"""
VendorGlobe API sync -- SQL Server connection config.

Every value can be overridden with an environment variable (same name,
VG_ prefix), so nothing needs editing for a server move:
    VG_DB_SERVER, VG_DB_NAME, VG_PORT, ...

Uses Windows Authentication (trusted connection) -- no password stored.
The Windows account running the app/service must have access to the
SQL Server instance below.
"""

import os


def _env(name, default):
    return os.environ.get("VG_" + name, default)


# --- SQL Server (192.168.66.28 = WIN-PJQA0USC6HT, SQL Server 2022) ---
DB_SERVER = _env("DB_SERVER", "192.168.66.28")
DB_NAME = _env("DB_NAME", "VendorGlobe_PR")

# Check "ODBC Data Sources (64-bit)" -> Drivers tab on the app server if
# this doesn't match what's installed. Driver 17 and 18 both work; 18
# requires TrustServerCertificate=yes (already set in the writers).
ODBC_DRIVER = _env("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

# --- HTTP port the Flask/Waitress app listens on ---
PORT = int(_env("PORT", "5002"))

# --- Source 1: PR report (full current list, no params) ---
SOURCE_URL = _env("SOURCE_URL", "https://smartworlddevelopersonline.com/SapPrReport.php")
TABLE_NAME = _env("TABLE_NAME", "PRReportHistory")
WRITE_INTERVAL_SECONDS = int(_env("WRITE_INTERVAL_SECONDS", "45"))

# --- Source 2: PR/NFA TAT report (?startdate=YYYY-MM-DD&enddate=...) ---
NFATAT_SOURCE_URL_BASE = _env(
    "NFATAT_SOURCE_URL_BASE",
    "https://smartworlddevelopersonline.com/SapPrNFATatReport.php",
)
NFATAT_TABLE_NAME = _env("NFATAT_TABLE_NAME", "PRNFATatReportHistory")
NFATAT_ROLLING_DAYS = int(_env("NFATAT_ROLLING_DAYS", "30"))
NFATAT_WRITE_INTERVAL_SECONDS = int(_env("NFATAT_WRITE_INTERVAL_SECONDS", "300"))

# Key column in the NFA TAT report's JSON (this report uses EPR_No,
# format like 0000010720 -- PR_No only exists in the other report).
NFATAT_PR_COLUMN = _env("NFATAT_PR_COLUMN", "EPR_No")
