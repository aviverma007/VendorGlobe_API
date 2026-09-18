-- ============================================================
-- PR -> PO journey: local mirror database on 192.168.66.28
-- Run in SSMS connected to 192.168.66.28 (or let the service
-- create it on first start - sap_sync.init() does the same).
-- ============================================================

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'PR2PO')
    CREATE DATABASE [PR2PO];
GO
USE [PR2PO];
GO

-- Mirror of SWDBIDB.dbo.PRD_PR (SAP PR line items)
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'SAP_PR')
CREATE TABLE dbo.SAP_PR (
    Banfn      varchar(20)  NOT NULL,  -- PR number (= VendorGlobe EPR_No)
    Bnfpo      varchar(10)  NOT NULL,  -- PR item
    Erdat      date NULL,              -- PR created
    Badat      date NULL,              -- requisition date
    Frgdt      date NULL,              -- PR released (SAP approval done)
    Bedat      date NULL,              -- PO date on the PR line
    RelStatus  varchar(20) NULL,
    Frgkz      char(1) NULL,           -- release indicator
    Frgst      varchar(5) NULL,        -- release strategy
    Frggr      varchar(5) NULL,        -- release group
    Statu      char(1) NULL,           -- N = not edited, B = PO created
    Loekz      bit NULL,               -- deleted flag
    Procstat   varchar(255) NULL,
    Ebeln      varchar(20) NULL,       -- follow-on PO number
    Ernam      varchar(50) NULL,       -- created by
    Afnam      varchar(50) NULL,       -- requisitioner
    Ekgrp      varchar(20) NULL,
    Eknam      varchar(255) NULL,      -- purchasing group name
    Ekorg      varchar(20) NULL,
    Werks      varchar(20) NULL,
    PlantDesc  varchar(255) NULL,
    Bsart      varchar(10) NULL,       -- document type
    Txz01      varchar(255) NULL,      -- short text
    Matkl      varchar(20) NULL,
    Menge      decimal(18,3) NULL,
    Netwr      decimal(18,2) NULL,     -- line value
    Bednr      varchar(20) NULL,
    Monat      varchar(2) NULL,
    Gjahr      varchar(4) NULL,
    fetched_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    first_seen DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_SAP_PR PRIMARY KEY (Banfn, Bnfpo)
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_sap_pr_erdat')
    CREATE INDEX ix_sap_pr_erdat ON dbo.SAP_PR (Erdat);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_sap_pr_ebeln')
    CREATE INDEX ix_sap_pr_ebeln ON dbo.SAP_PR (Ebeln);
GO

-- Mirror of SWDBIDB.dbo.PRD_PurchaseOrder (PO line items)
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'SAP_PO')
CREATE TABLE dbo.SAP_PO (
    EBELN      varchar(20) NOT NULL,   -- PO number
    EBELP      int         NOT NULL,   -- PO item
    BADAT      date NULL,              -- PO document date (created)
    AEDAT      date NULL,              -- last change date
    KDATB      date NULL,
    KDATE      date NULL,
    FRGZU      varchar(10) NULL,       -- release levels granted ('X','XX',...)
    FRGKE      varchar(10) NULL,       -- release indicator (B blocked, G released)
    PROCSTAT   varchar(20) NULL,       -- 03 in release, 05 released
    LOEKZ      char(1) NULL,
    NAME1      varchar(100) NULL,      -- vendor name
    BSART      varchar(10) NULL,
    EKGRP      varchar(10) NULL,
    EKNAM      varchar(100) NULL,
    EKORG      varchar(10) NULL,
    WERKS      varchar(10) NULL,
    PLANT_DESC varchar(100) NULL,
    MATKL      varchar(50) NULL,
    TXZ01      varchar(255) NULL,
    MENGE      decimal(18,3) NULL,
    NETPR      decimal(18,2) NULL,
    NETWR      decimal(18,2) NULL,
    MENGE_DEL  decimal(18,3) NULL,
    MENGE_INV  decimal(18,3) NULL,
    NETWR_INV  decimal(18,2) NULL,
    WAERS      varchar(5) NULL,
    MONAT      int NULL,
    GJAHR      int NULL,
    fetched_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    first_seen DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_SAP_PO PRIMARY KEY (EBELN, EBELP)
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_sap_po_badat')
    CREATE INDEX ix_sap_po_badat ON dbo.SAP_PO (BADAT);
GO

SELECT 'PR2PO ready' AS status;
