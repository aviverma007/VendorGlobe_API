# VendorGlobe_API

Sync service: pulls the VendorGlobe SAP PR APIs into SQL Server, one row
per PR (upsert - insert new, update changed, skip identical), and serves
JSON/OData endpoints for SAP to consume.

- Sources: SapPrReport.php (45s cycle) + SapPrNFATatReport.php (300s, rolling 30 days)
- DB: 192.168.66.28 / VendorGlobe_PR (auto-created; tables keyed on PR_No / EPR_No)
- Server: Waitress on port 5002, run as Windows service "VendorGlobeAPI" (NSSM)

## Quick start (app server, Admin PowerShell)
```powershell
cd D:\
git clone https://github.com/aviverma007/VendorGlobe_API.git
cd VendorGlobe_API
# put nssm.exe in this folder (or edit $Nssm in setup.ps1), then:
.\setup.ps1
```

## Endpoints
| Path | Purpose |
|---|---|
| /health | DB status, row counts, last write times |
| /check_pr?pr=N | PR lookup (SAP SM59) |
| /api/data | full current PR list (SAP 5-min job) |
| /nfatat/check_pr?pr=N | EPR lookup |
| /nfatat/search?startdate=&enddate= | NFA TAT range search |
| /odata/PRReportHistory | OData feed |
| / | live browser table |

Config lives in app/db_config.py; every value overridable via VG_* env vars.
See RUNBOOK.md for operations and troubleshooting.
