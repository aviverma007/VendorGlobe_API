-- ============================================================
-- Read-only SQL login for the PR2PO sync.
-- Run in SSMS connected to 192.168.66.33 (the SAP BI server).
-- CHANGE THE PASSWORD before running; then set on the .28 box:
--   setx VG_SAP_DB_USER pr2po_reader /M
--   setx VG_SAP_DB_PASSWORD "<the password>" /M
-- and restart the VendorGlobeAPI service.
-- ============================================================

USE [master];
GO
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'pr2po_reader')
    CREATE LOGIN [pr2po_reader] WITH PASSWORD = 'CHANGE_ME_Strong#2026',
        CHECK_POLICY = ON, CHECK_EXPIRATION = OFF;
GO
USE [SWDBIDB];
GO
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'pr2po_reader')
    CREATE USER [pr2po_reader] FOR LOGIN [pr2po_reader];
GO
-- Read-only, and only the two tables the sync needs.
GRANT SELECT ON dbo.PRD_PR TO [pr2po_reader];
GRANT SELECT ON dbo.PRD_PurchaseOrder TO [pr2po_reader];
GO
SELECT 'pr2po_reader ready (read-only on PRD_PR, PRD_PurchaseOrder)' AS status;
