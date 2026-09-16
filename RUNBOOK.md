# VendorGlobe_API — Operations Runbook

Last updated: 02-Sep-2026 (post 10 GB incident + upsert redesign + Waitress)

## 1. What this system is

Python service on **the app server**, port **5002**, run as Windows
service **VendorGlobeAPI** via NSSM. It:

- Fetches the SAP PR report every **45s** and the NFA TAT report every **300s**
- **Upserts** into SQL Server (**192.168.66.28**, DB `VendorGlobe_PR`):
  one row per `PR_No` in `PRReportHistory`, one row per `EPR_No` in
  `PRNFATatReportHistory`. New PR -> INSERT, changed PR -> UPDATE,
  identical -> no write. Growth: ~5-10 rows/day.
- Serves `/check_pr`, `/nfatat/check_pr`, `/nfatat/search`, `/api/data`,
  `/odata/*` for SAP (SM59 destinations ZPR_CHECK_TEST / ZPR_CHECK_LIST)
  and the live browser table at `/`.
- Web server: **Waitress** (8 threads). Access log written by the app itself.

Key paths on server 34:

| Thing | Path |
|---|---|
| App | `D:\VendorGlobe_API\app\app.py` |
| Python | `D:\python312\python.exe` |
| NSSM | `D:\nssm\nssm-2.24\win64\nssm.exe` |
| Activity log | `app\service_stdout.log` |
| Access log | `app\access.log` (rotating 5 MB x3) |
| Error log | `app\service_stderr.log` |

## 2. Daily operation

Nothing is required daily. The service self-runs and NSSM auto-restarts it on
crash (5s throttle). Optional 30-second health glance:

```powershell
Get-Service VendorGlobeAPI                                                # Running?
Get-Content D:\VendorGlobe_API\app\service_stdout.log -Tail 5   # any ERROR?
curl "http://localhost:5002/check_pr?pr=8110000659" -UseBasicParsing      # 200?
```

Healthy log lines look like:
```
[db_writer] 14:54:42 +1 new, ~0 updated, 716 unchanged
[nfa_tat_writer] 15:06:18 +0 new, ~1 updated, 234 unchanged (...)
```
Silence between lines is normal - the writers only print when something changed.

## 3. Deploying a code change

```powershell
# Admin PowerShell on server 34
cd D:\VendorGlobe_API
git pull
Restart-Service VendorGlobeAPI
Start-Sleep 5
Get-Content app\service_stdout.log -Tail 5    # expect the Waitress banner
```

`Restart-Service` failing with "Cannot open VendorGlobeAPI service" = you are NOT in
an admin window. `Start-Process powershell -Verb RunAs` and retry.

## 4. Fresh install / rebuild of the service

```powershell
# Admin PowerShell
cd D:\smartdesk_live
git clone https://github.com/aviverma007/VendorGlobe_API.git
D:\python312\python.exe -m pip install -r VendorGlobe_API\requirements.txt

$nssm = "D:\nssm\nssm-2.24\win64\nssm.exe"
& $nssm install VendorGlobeAPI "D:\python312\python.exe" "D:\VendorGlobe_API\app\app.py"
& $nssm set VendorGlobeAPI AppDirectory "D:\VendorGlobe_API\app"
& $nssm set VendorGlobeAPI AppStdout "D:\VendorGlobe_API\app\service_stdout.log"
& $nssm set VendorGlobeAPI AppStderr "D:\VendorGlobe_API\app\service_stderr.log"
& $nssm set VendorGlobeAPI AppEnvironmentExtra "PYTHONUNBUFFERED=1"
& $nssm set VendorGlobeAPI AppThrottle 5000
Start-Service VendorGlobeAPI
```

The app auto-creates the database and tables if missing (PR_No / EPR_No primary
keys + fetched_at + first_seen). No SQL scripts needed for a fresh start.

## 5. Incident history and fixes (read before "fixing" anything)

### 5.1 The 10 GB incident (Aug 25-27, 2026) - RESOLVED
- Old design snapshotted the FULL report every 45s -> 1.2M rows/day ->
  SQL Server **Express** hit its hard **10 GB per-database cap** -> every write
  failed with `filegroup is full` -> new PRs stopped appearing in SAP.
- Fix: writers rewritten to upsert; bloated tables dropped and rebuilt
  (29.6M rows -> ~717). DB now ~4 MB, grows ~130 KB/year.
- If `filegroup is full` EVER returns, something re-introduced bulk inserts -
  check recent code changes; do NOT try to raise the cap (Express won't allow it).

### 5.2 NIECONN_REFUSED from SAP - ONGOING, NETWORK-SIDE, NOT FIXABLE HERE
Symptom: SM59 tests / the 5-min ZPR job alternate SUCCESS and
`NIECONN_REFUSED(-10)` in 10-25 minute blocks. Evidence collected 02-Sep:
- SAP 5-min probe flapped all morning (exact transitions: 10:27 fail,
  10:57 ok, 11:12 fail, 11:32 ok, 11:37 fail, 11:47 ok, 11:52 fail,
  12:07 ok, 12:17 fail, 12:27 ok).
- Access log shows a dashboard client (192.168.10.x) getting 200 EVERY
  MINUTE through the same windows -> server was continuously reachable
  on another path.
- During SAP failure windows, ZERO packets from 26.x arrive -> requests
  are refused on the network path, not by the app.
- Server 34's Windows Firewall is DISABLED (all profiles) -> no host-level
  filtering exists at all.
- SM59 proxy bypass for 192.168.66.34 configured 27-08 -> failures are on
  the DIRECT path.

Conclusion: unstable network path between SAP zone (192.168.26.x) and
192.168.66.x - likely HA firewall flapping / dual-path routing with
inconsistent rules. **Only the network team can fix this.** Escalation must
include the transition timestamps above so they can match their event logs.

DO NOT, when this recurs: restart VendorGlobeAPI, change the port, or edit
server 34 firewall rules. All were verified irrelevant repeatedly.

Note: source IPs seen from SAP are 192.168.26.26 / .34, NOT the registered
system IPs (DS4=.35, QS4=.28, PS4=.31) - NAT or separate app servers.
Any firewall request must cover 192.168.26.0/24, not single IPs.

### 5.3 Windows Firewall state
Disabled on all profiles (discovered 01-Sep). A correctly-scoped allow rule
"QMS PR Report App" exists but is NOT being enforced. Decision on enabling
belongs to infra (risk: locking out RDP). If enabling, first add
192.168.10.0/24 (dashboard users) to the rule and verify the RDP rule.

### 5.4 NFA TAT duplicate keys
The NFA TAT source report legitimately contains the same EPR_No on multiple
rows. The writer dedupes per batch (keeps last occurrence). A PK violation
on PRNFATatReportHistory means this dedup was removed - restore it.

## 6. Troubleshooting quick table

| Symptom | Cause | Action |
|---|---|---|
| SAP: NIECONN_REFUSED, access log silent | Network path flap (5.2) | Escalate to network team with timestamps; wait; job self-heals next cycle |
| `filegroup is full` in stdout log | Bulk inserts reintroduced (5.1) | Check code; tables should stay ~717 rows |
| `Cannot open VendorGlobeAPI service` | Non-admin PowerShell | Run as administrator |
| Port 5002 not listening | Service down and NSSM gave up | `Start-Service VendorGlobeAPI`; check service_stderr.log |
| "development server" warning in log | waitress not installed | `python.exe -m pip install waitress`, restart |
| access.log missing | Never written yet or path changed | Restart service; check AppDirectory in NSSM |
| PK violation EPR_No | Batch dedup removed (5.4) | Restore dedup in nfa_tat_writer.py |
| New PR not in /check_pr after 2 min | Writer erroring | Check service_stdout.log tail |

## 7. SQL Server notes (192.168.66.28)

- **Express edition: 10 GB hard cap per database.** Current size ~4 MB.
- Windows Authentication (trusted connection) from server 34.
- Check size: `SELECT name, size*8/1024 AS MB FROM VendorGlobe_PR.sys.database_files;`
- Tables keep only the LATEST state per PR (plus first_seen). Historical
  field-change audit was intentionally dropped in the redesign.

## 8. Open items

- [ ] Network escalation for SAP path flapping (5.2) - owner: Anirudh -> infra
- [ ] Firewall-disabled disclosure to infra/HOD (5.3)
- [ ] Revoke GitHub PAT used during Aug-Sep sessions
- [ ] Optional: /health endpoint, nightly DB backup to E:, change-audit table
