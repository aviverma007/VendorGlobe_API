-- ============================================================
-- VendorGlobe_PR bootstrap -- run once in SSMS on 192.168.66.28
-- (The app also auto-creates all of this on first start; this
--  script just lets you do it explicitly / review it.)
-- ============================================================

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'VendorGlobe_PR')
    CREATE DATABASE VendorGlobe_PR;
GO

USE VendorGlobe_PR;
GO

-- One row per PR. Data columns are added automatically by the app
-- from the source JSON (NVARCHAR(1000) each).
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PRReportHistory')
CREATE TABLE dbo.PRReportHistory (
    PR_No      NVARCHAR(100) NOT NULL PRIMARY KEY,
    fetched_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),  -- last seen/changed
    first_seen DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_prh_fetched_at')
    CREATE INDEX ix_prh_fetched_at ON dbo.PRReportHistory (fetched_at);
GO

-- One row per EPR. Data columns auto-added (NVARCHAR(MAX) each).
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PRNFATatReportHistory')
CREATE TABLE dbo.PRNFATatReportHistory (
    EPR_No     NVARCHAR(100) NOT NULL PRIMARY KEY,
    fetched_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    first_seen DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_nfatat_fetched_at')
    CREATE INDEX ix_nfatat_fetched_at ON dbo.PRNFATatReportHistory (fetched_at);
GO

SELECT 'VendorGlobe_PR ready' AS status;
