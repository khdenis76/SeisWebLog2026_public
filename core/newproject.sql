--Create file tables
CREATE TABLE  IF NOT EXISTS  Files (
                                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                FileName TEXT UNIQUE,
                                UNIQUE(FileName, ID)
                               );
--Create solution table
CREATE TABLE  IF NOT EXISTS  Solutions (
                                   ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                   Solution TEXT NOT NULL,
                                   UNIQUE(Solution)
                                  );
CREATE TABLE  IF NOT EXISTS  project_vessels (
                           ID INTEGER PRIMARY KEY AUTOINCREMENT,
                           Name TEXT NOT NULL UNIQUE,
                           Description TEXT,
                           IMONum TEXT NOT NULL UNIQUE
                       );
CREATE TABLE  IF NOT EXISTS  ROVS_CONFIG

                (
                 "rov1_name" TEXT DEFAULT 'ROV1',
                 "rov2_name" TEXT DEFAULT 'ROV2',
                 "gnss1_name" TEXT DEFAULT 'GNSS1',
                 "gnss2_name" TEXT DEFAULT 'GNSS2'
                );
--Create receiver preplot tables
CREATE TABLE  IF NOT EXISTS  RLPreplot (
                              ID INTEGER PRIMARY KEY AUTOINCREMENT,
                              Line INTEGER,
                              TierLine INTEGER UNIQUE, -- Unique constraint added to TierLine column
                              Points INTEGER,
                              UPoints INTEGER,
                              FirstPoint INTEGER,
                              LastPoint INTEGER,
                              MinPoint INTEGER,
                              MaxPoint INTEGER,
                              RealStartX REAL,
                              RealStartY REAL,
                              RealEndX REAL,
                              RealEndY REAL,
                              StartX REAL,
                              StartY REAL,
                              EndX REAL,
                              EndY REAL,
                              LineLength REAL DEFAULT 0,
                              RealLineLength REAL DEFAULT 0,
                              LineBearing REAL DEFAULT 0,
                              CalcLineBearing REAL DEFAULT 0,

                              isLineClicked INTEGER DEFAULT 0,
                              isLineDeployed INTEGER DEFAULT 0,
                              isLinePinged INTEGER DEFAULT 0,
                              isLineRecovered INTEGER DEFAULT 0,
                              isMessaged INTEGER DEFAULT 0,
                              isValidated INTEGER DEFAULT 0,

                              RPIndex INTEGER DEFAULT 0,
                              Tier INTEGER DEFAULT 1,

                              PointsDep INTEGER DEFAULT 0,
                              PointsRec INTEGER DEFAULT 0,
                              PointsProc INTEGER DEFAULT 0,

                              Comments TEXT DEFAULT '',
                              Message Text DEFAULT '',
                              ValidationTime TEXT,

                              Spare1 INTEGER DEFAULT 0,
                              Spare2 INTEGER DEFAULT 0,
                              Spare3 INTEGER DEFAULT 0,

                              Spare4 REAL DEFAULT 0,
                              Spare5 REAL DEFAULT 0,
                              Spare6 REAL DEFAULT 0,

                              File_FK INTEGER,
                              FOREIGN KEY (File_FK) REFERENCES Files(ID)  ON DELETE CASCADE,
                              UNIQUE (TierLine, ID)
                         );
CREATE TABLE  IF NOT EXISTS  RPPreplot (
                              ID INTEGER PRIMARY KEY AUTOINCREMENT,
                              Line_FK INTEGER,
                              Line INTEGER,
                              TierLine INTEGER,
                              Point INTEGER,
                              PointCode TEXT DEFAULT '',
                              PointIndex INTEGER DEFAULT 1,
                              LinePoint INTEGER DEFAULT 0,
                              TLinePoint REAL,
                              LinePointIndex REAL DEFAULT 0,
                              TLinePointIndex REAL DEFAULT 0,
                              X REAL,
                              Y REAL,
                              Z REAL,
                              LineBearing REAL DEFAULT 0,
                              Tier INTEGER DEFAULT 1,
                              Spare1 INTEGER DEFAULT 0,
                              Spare2 INTEGER DEFAULT 0,
                              Spare3 INTEGER DEFAULT 0,
                              File_FK INTEGER,
                              FOREIGN KEY (Line_FK) REFERENCES RLPreplot(ID) ON DELETE CASCADE,
                              FOREIGN KEY (File_FK) REFERENCES Files(ID) ON DELETE CASCADE,
                              UNIQUE(ID)
                            );
CREATE UNIQUE INDEX IF NOT EXISTS ux_rppreplot
       ON RPPreplot (Tier, Line, Point, PointIndex);
--Create source preplot tables
CREATE TABLE  IF NOT EXISTS  SLPreplot (
                              ID INTEGER PRIMARY KEY AUTOINCREMENT,
                              Line INTEGER,
                              TierLine INTEGER UNIQUE, -- Unique constraint added to TierLine column
                              Points INTEGER,
                              UPoints INTEGER,
                              FirstPoint INTEGER,
                              LastPoint INTEGER,
                              MinPoint INTEGER,
                              MaxPoint INTEGER,
                              RealStartX REAL,
                              RealStartY REAL,
                              RealEndX REAL,
                              RealEndY REAL,
                              StartX REAL,
                              StartY REAL,
                              EndX REAL,
                              EndY REAL,
                              LineLength REAL DEFAULT 0,
                              RealLineLength REAL DEFAULT 0,
                              LineBearing REAL DEFAULT 0,
                              CalcLineBearing REAL DEFAULT 0,

                              isLineClicked INTEGER DEFAULT 0,
                              isLineDeployed INTEGER DEFAULT 0,
                              isLinePinged INTEGER DEFAULT 0,
                              isLineRecovered INTEGER DEFAULT 0,
                              isMessaged INTEGER DEFAULT 0,
                              isValidated INTEGER DEFAULT 0,

                              RPIndex INTEGER DEFAULT 0,
                              Tier INTEGER DEFAULT 1,

                              PointsDep INTEGER DEFAULT 0,
                              PointsRec INTEGER DEFAULT 0,
                              PointsProc INTEGER DEFAULT 0,

                              Comments TEXT DEFAULT '',
                              Message Text DEFAULT '',
                              ValidationTime TEXT,

                              Spare1 INTEGER DEFAULT 0,
                              Spare2 INTEGER DEFAULT 0,
                              Spare3 INTEGER DEFAULT 0,

                              Spare4 REAL DEFAULT 0,
                              Spare5 REAL DEFAULT 0,
                              Spare6 REAL DEFAULT 0,

                              File_FK INTEGER,
                              FOREIGN KEY (File_FK) REFERENCES Files(ID)  ON DELETE CASCADE,
                              UNIQUE (TierLine, ID)
                         );
CREATE TABLE  IF NOT EXISTS  SPPreplot (
                              ID INTEGER PRIMARY KEY AUTOINCREMENT,
                              Line_FK INTEGER,
                              Line INTEGER,
                              TierLine INTEGER,
                              Point INTEGER,
                              PointCode TEXT DEFAULT '',
                              PointIndex INTEGER DEFAULT 1,
                              LinePoint INTEGER DEFAULT 0,
                              TLinePoint REAL,
                              LinePointIndex REAL DEFAULT 0,
                              TLinePointIndex REAL DEFAULT 0,
                              X REAL,
                              Y REAL,
                              Z REAL,
                              LineBearing REAL DEFAULT 0,
                              Tier INTEGER DEFAULT 1,
                              Spare1 INTEGER DEFAULT 0,
                              Spare2 INTEGER DEFAULT 0,
                              Spare3 INTEGER DEFAULT 0,
                              File_FK INTEGER,
                              FOREIGN KEY (Line_FK) REFERENCES SLPreplot(ID) ON DELETE CASCADE,
                              FOREIGN KEY (File_FK) REFERENCES Files(ID) ON DELETE CASCADE,
                              UNIQUE(ID)
                            );
CREATE UNIQUE INDEX IF NOT EXISTS ux_sppreplot
       ON SPPreplot (Tier, Line, Point, PointIndex);
-- for fast WHERE Line_FK = ?
CREATE INDEX IF NOT EXISTS ix_sppreplot_linefk ON SPPreplot(Line_FK);
CREATE INDEX IF NOT EXISTS ix_rppreplot_linefk ON RPPreplot(Line_FK);
-- for fast ORDER BY Point and fast MIN/MAX by Point
CREATE INDEX IF NOT EXISTS ix_sppreplot_linefk_point ON SPPreplot(Line_FK, Point);
CREATE INDEX IF NOT EXISTS ix_rppreplot_linefk_point ON RPPreplot(Line_FK, Point);
CREATE TABLE IF NOT EXISTS "DSRSolution" (
    "ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "NAME" TEXT NOT NULL DEFAULT 'Normal'
);
INSERT OR IGNORE INTO "DSRSolution"(ID, NAME)
VALUES (1, 'Normal');
INSERT OR IGNORE INTO "DSRSolution"(ID, NAME)
VALUES (2, 'Co-deployed');
INSERT OR IGNORE INTO "DSRSolution"(ID, NAME)
VALUES (3, 'Losted');
INSERT OR IGNORE INTO "DSRSolution"(ID, NAME)
VALUES (4, 'Missplaced');
INSERT OR IGNORE INTO "DSRSolution"(ID, NAME)
VALUES (5, 'Wrong ID');
INSERT OR IGNORE INTO "DSRSolution"(ID, NAME)
VALUES (6, 'Overlap');
--Create DSR table
CREATE TABLE IF NOT EXISTS "DSR" (
    "ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "Solution_FK" INTEGER NOT NULL DEFAULT 1,
    "RLPreplot_FK" INTEGER,
    "LinePointIdx" INTEGER,
    "Line" INTEGER,
    "Station" INTEGER,
    "Node" TEXT,
    "NODE_HEX_ID" INT NOT NULL,
    "PreplotEasting" REAL,
    "PreplotNorthing" REAL,
    "ROV" TEXT,
    "TimeStamp" TEXT,
    "PrimaryEasting" REAL,
    "Sigma" REAL,
    "PrimaryNorthing" REAL,
    "Sigma1" REAL,
    "SecondaryEasting" REAL,
    "Sigma2" REAL,
    "SecondaryNorthing" REAL,
    "Sigma3" REAL,
    "DeltaEprimarytosecondary" REAL,
    "DeltaNprimarytosecondary" REAL,
    "Rangeprimarytosecondary" REAL,
    "RangetoPrePlot" REAL,
    "BrgtoPrePlot" REAL,
    "PrimaryElevation" REAL,
    "Sigma4" REAL,
    "SecondaryElevation" REAL,
    "Sigma5" REAL,
    "Quality" TEXT,
    "ROV1" TEXT,
    "TimeStamp1" TEXT,
    "PrimaryEasting1" REAL,
    "Sigma6" REAL,
    "PrimaryNorthing1" REAL,
    "Sigma7" REAL,
    "SecondaryEasting1" REAL,
    "Sigma8" REAL,
    "SecondaryNorthing1" REAL,
    "Sigma9" REAL,
    "DeltaEprimarytosecondary1" REAL,
    "DeltaNprimarytosecondary1" REAL,
    "Rangeprimarytosecondary1" REAL,
    "RangetoPrePlot1" REAL,
    "BrgtoPrePlot1" REAL,
    "PrimaryElevation1" REAL,
    "Sigma10" REAL,
    "SecondaryElevation1" REAL,
    "Sigma11" REAL,
    "Quality1" TEXT,
    "DeployedtoRetrievedEasting" REAL,
    "DeployedtoRetrievedNorthing" REAL,
    "DeployedtoRecoveredElevation" REAL,
    "DeployedtoRetrievedRange" REAL,
    "DeployedtoRetrievedBrg" REAL,
    "Comments" TEXT,
    "LinePoint" INTEGER,
    "RecIdx" INTEGER DEFAULT 1,
    "Date" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "Year" INTEGER,
    "Month" INTEGER,
    "Week" INTEGER,
    "Day" TEXT,
    "JDay" INTEGER DEFAULT 0,
    "Date1" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "Year1" INTEGER,
    "Month1" INTEGER,
    "Week1" INTEGER,
    "Day1" TEXT,
    "JDay1" INTEGER DEFAULT 0,
    "DepTime" INTEGER,
    "RecTime" INTEGER,
    "PointComment" TEXT,
    "TIER" INTEGER DEFAULT 1,
    "isExported" INTEGER DEFAULT 0,
    "isRecExported" INTEGER DEFAULT 0,
    "Area" TEXT,
    "RemoteUnit" TEXT,
    "AUQRCode" TEXT,
    "AURFID" TEXT,
    "CUSerialNumber" TEXT,
    "Status" TEXT,
    "DeploymentType" TEXT,
    "StartTimeEpoch" INTEGER,
    "StartTimeUTC" TEXT,
    "DeployTimeEpoch" INTEGER,
    "DeployTimeUTC" TEXT,
    "PickupTimeEpoch" INTEGER,
    "PickupTimeUTC" TEXT,
    "StopTimeEpoch" INTEGER,
    "StopTimeUTC" TEXT,
    "SPSX" REAL,
    "SPSY" REAL,
    "SPSZ" REAL,
    "ActualX" REAL,
    "ActualY" REAL,
    "ActualZ" REAL,
    "Deployed" TEXT,
    "PickedUp" TEXT,
    "Archived" TEXT,
    "DeviceID" INTEGER,
    "BinID" INTEGER,
    "ExpectedTraces" INTEGER,
    "CollectedTraces" INTEGER,
    "DownloadedDatainMB" INTEGER,
    "ExpectedDatainMB" INTEGER,
    "DownloadError" INTEGER,
    "DaysInWater" INTEGER DEFAULT 0,
    "TodayDaysInWater" INTEGER DEFAULT 0,
    FOREIGN KEY ("Solution_FK") REFERENCES "DSRSolution"("ID") ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY ("RLPreplot_FK") REFERENCES "RLPreplot"("ID") ON UPDATE CASCADE ON DELETE SET NULL,
    CONSTRAINT "ux_dsr_line_station_node" UNIQUE ("Line","Station","NODE_HEX_ID")
);

CREATE INDEX IF NOT EXISTS "ix_dsr_line_station"
    ON "DSR"("Line","Station");

CREATE INDEX IF NOT EXISTS "ix_dsr_node"
    ON "DSR"("NODE_HEX_ID");

CREATE INDEX IF NOT EXISTS "ix_dsr_recidx"
    ON "DSR"("RecIdx");

CREATE INDEX IF NOT EXISTS "ix_dsr_tier"
    ON "DSR"("TIER");

CREATE INDEX IF NOT EXISTS "ix_dsr_timestamp"
    ON "DSR"("TimeStamp");

CREATE INDEX IF NOT EXISTS "ix_dsr_timestamp1"
    ON "DSR"("TimeStamp1");
CREATE INDEX IF NOT EXISTS ix_dsr_rlpreplot_fk
ON DSR(RLPreplot_FK);
--create rec_db table for fb data
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS REC_DB
(
    ID INTEGER PRIMARY KEY AUTOINCREMENT,

    -- File link (source rec_db file)
    File_FK     INTEGER,

    -- Link to RLPreplot geometry
    Preplot_FK  INTEGER,

    -- Keys from REC_DB file
    REC_ID   INTEGER,
    NODE_ID  INTEGER,
    DEPLOY   INTEGER,
    RPI      INTEGER,
    PART_NO  INTEGER,

    -- Geometry fields
    Line              INTEGER,
    Point             INTEGER,
    LinePoint         INTEGER,
    LinePointIdx      INTEGER,

    TierLine          INTEGER,
    TierLinePoint     INTEGER,
    TierLinePointIdx  INTEGER,

    -- Coordinates
    RPRE_X   REAL,
    RPRE_Y   REAL,
    RFIELD_X REAL,
    RFIELD_Y REAL,
    RFIELD_Z REAL,
    REC_X    REAL,
    REC_Y    REAL,
    REC_Z    REAL,

    -- Timing / drift
    TIMECORR REAL,
    BULKSHFT REAL,
    QDRIFT   REAL,
    LDRIFT   REAL,

    TRIMPTCH REAL,
    TRIMROLL REAL,
    TRIMYAW  REAL,

    PITCHFIN REAL,
    ROLLFIN  REAL,
    YAWFIN   REAL,

    TOTDAYS  REAL,
    RECCOUNT INTEGER,
    CLKFLAG  INTEGER,

    EC1_RUS0 REAL    DEFAULT 0,
    EC1_RUS1 REAL    DEFAULT 0,
    EC1_EDT0 REAL    DEFAULT 0,
    EC1_EDT1 REAL    DEFAULT 0,
    EC1_EPT0 REAL    DEFAULT 0,
    EC1_EPT1 REAL    DEFAULT 0,

    NODSTART INTEGER DEFAULT 0,
    DEPLOYTM INTEGER DEFAULT 0,
    PICKUPTM INTEGER DEFAULT 0,
    RUNTIME  INTEGER DEFAULT 0,

    EC2_CD1  INTEGER DEFAULT 0,
    TOTSHOTS INTEGER DEFAULT 0,
    TOTPROD  INTEGER DEFAULT 0,
    SPSK     INTEGER DEFAULT 0,

    -- Tier level (default 1)
    TIER     INTEGER DEFAULT 1,

    -- Composite uniqueness rule
    UNIQUE (REC_ID, DEPLOY, RPI),

    -- Foreign keys
    FOREIGN KEY (Preplot_FK)
        REFERENCES RLPreplot(ID)
        ON DELETE SET NULL
        ON UPDATE CASCADE,

    FOREIGN KEY (File_FK)
        REFERENCES Files(ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

--create black box table (rov_box)
CREATE TABLE IF NOT EXISTS "BBox_Configs_List" (
	"ID"	INTEGER,
	"Name"	TEXT NOT NULL,
	"IsDefault"	INTEGER DEFAULT 0,
	"rov1_name"	TEXT,
	"rov2_name"	TEXT,
	"gnss1_name"	TEXT,
	"gnss2_name"	Text,
	"Vessel_name"	TEXT,
	"Depth1_name"	TEXT,
	"Depth2_name"	TEXT,
	PRIMARY KEY("ID" AUTOINCREMENT),
	CONSTRAINT "ux_bbox_configs_name" UNIQUE("Name")
);
CREATE TRIGGER IF NOT EXISTS trg_bbox_default_singleton
AFTER UPDATE OF IsDefault ON BBox_Configs_List
WHEN NEW.IsDefault = 1
BEGIN
    UPDATE BBox_Configs_List
    SET IsDefault = 0
    WHERE ID != NEW.ID;
END;
CREATE TRIGGER IF NOT EXISTS trg_bbox_default_singleton_ins
AFTER INSERT ON BBox_Configs_List
WHEN NEW.IsDefault = 1
BEGIN
    UPDATE BBox_Configs_List
    SET IsDefault = 0
    WHERE ID != NEW.ID;
END;
CREATE TABLE IF NOT EXISTS BBox_Config (
    ID INTEGER PRIMARY KEY,
    FieldName TEXT NOT NULL,
    FileColumn TEXT,
    inUse INTEGER DEFAULT 0,
    CONFIG_FK INTEGER NOT NULL,
    FOREIGN KEY (CONFIG_FK) REFERENCES BBox_Configs_List(ID)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    CONSTRAINT ux_bbox_config_cfg_field UNIQUE (CONFIG_FK, FieldName)
);
CREATE TABLE IF NOT EXISTS BlackBox_Files (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            FileName TEXT NOT NULL,
            Config_FK INTEGER,
            UploadedAt TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (Config_FK) REFERENCES BBox_Configs_List(ID) ON DELETE CASCADE,
            UNIQUE(FileName, Config_FK)
        );
CREATE TABLE IF NOT EXISTS BlackBox (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,

            TimeStamp TEXT,

            VesselEasting REAL,
            VesselNorthing REAL,
            VesselElevation REAL,
            VesselHDG REAL,
            VesselSOG REAL,
            VesselCOG REAL,

            GNSS1_Easting REAL,
            GNSS1_Northing REAL,
            GNSS1_Elevation REAL,
            GNSS2_Easting REAL,
            GNSS2_Northing REAL,
            GNSS2_Elevation REAL,

            ROV1_INS_Easting REAL,
            ROV1_INS_Northing REAL,
            ROV1_USBL_Easting REAL,
            ROV1_USBL_Northing REAL,
            ROV1_Depth REAL,
            ROV1_HDG REAL,
            ROV1_SOG REAL,
            ROV1_COG REAL,
            ROV1_TMS_Easting REAL,
            ROV1_TMS_Northing REAL,
            ROV1_TMS_Depth REAL,

            ROV2_INS_Easting REAL,
            ROV2_INS_Northing REAL,
            ROV2_USBL_Easting REAL,
            ROV2_USBL_Northing REAL,
            ROV2_Depth REAL,
            ROV2_HDG REAL,
            ROV2_SOG REAL,
            ROV2_COG REAL,
            ROV2_TMS_Easting REAL,
            ROV2_TMS_Northing REAL,
            ROV2_TMS_Depth REAL,

            Crane_Easting REAL,
            Crane_Northing REAL,
            Crane_Depth REAL,

            GNSS1_RefStation TEXT,
            GNSS1_NOS INTEGER,
            GNSS1_DiffAge REAL,
            GNSS1_FixQuality INTEGER,
            GNSS1_HDOP REAL,
            GNSS1_PDOP REAL,
            GNSS1_VDOP REAL,

            GNSS2_RefStation TEXT,
            GNSS2_NOS INTEGER,
            GNSS2_DiffAge REAL,
            GNSS2_FixQuality INTEGER,
            GNSS2_HDOP REAL,
            GNSS2_PDOP REAL,
            GNSS2_VDOP REAL,

            ROV1_PITCH REAL,
            ROV1_ROLL REAL,
            ROV2_PITCH REAL,
            ROV2_ROLL REAL,

            ROV1_Depth1 REAL,
            ROV1_Depth2 REAL,
            ROV2_Depth1 REAL,
            ROV2_Depth2 REAL,

            Barometer REAL,

            File_FK INTEGER,
            FOREIGN KEY (File_FK) REFERENCES BlackBox_Files(ID) ON DELETE CASCADE
        );
CREATE INDEX IF NOT EXISTS idx_blackbox_ts ON BlackBox(TimeStamp);
CREATE INDEX IF NOT EXISTS idx_blackbox_file ON BlackBox(File_FK);

--Create Source Solution
CREATE TABLE  IF NOT EXISTS  SLSolution (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        PPLine_FK INTEGER DEFAULT 0,
        File_FK INTEGER,
        SailLine TEXT UNIQUE,
        Line INTEGER,
        Seq INTEGER,
        Attempt TEXT,
        Tier INTEGER,
        TierLine INTEGER,
        FSP INTEGER,
        LSP INTEGER,
        FGSP INTEGER,
        LGSP INTEGER,
        StartX DOUBLE,
        StartY DOUBLE,
        EndX DOUBLE,
        EndY DOUBLE,
        Vessel_FK INTEGER,
        Start_Time DATETIME,
        End_Time DATETIME,
        LineLength REAL DEFAULT 0,
        Start_Production_Time DATETIME,
        End_Production_Time DATETIME,
        PercentOfLineCompleted REAL,
        PercentOfSeqCompleted REAL,
        ProductionCount INTEGER,
        NonProductionCount INTEGER,
        KillCount INTEGER,
        MinGunDepth REAL,
        MaxGunDepth REAL,
        MinProdGunDepth REAL DEFAULT 0,
        MaxProdGunDepth REAL DEFAULT 0,
        MinNonProdGunDepth REAL DEFAULT 0,
        MaxNonProdGunDepth REAL DEFAULT 0,
        MinWaterDepth REAL,
        MaxWaterDepth REAL,
        MinProdWaterDepth REAL,
        MaxProdWaterDepth REAL,
        MinNonProdWaterDepth REAL,
        MaxNonProdWaterDepth REAL,
        MinKillGunDepth REAL,
        MaxKillGunDepth REAL,
        MinKillWaterDepth REAL,
        MaxKillWaterDepth REAL,
        PP_Length REAL,
        SeqLenPercentage REAL,
        MaxSPI REAL,
        MaxSeq INTEGER,
        purpose_id INTEGER,
        FOREIGN KEY (Vessel_FK) REFERENCES project_fleet(ID) ON DELETE CASCADE ON UPDATE CASCADE,
        FOREIGN KEY (PPLine_FK) REFERENCES SLPreplot(ID)  ON UPDATE CASCADE,
        FOREIGN KEY (File_FK) REFERENCES Files(ID) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE  IF NOT EXISTS  SPSolution (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    SailLine_FK INTEGER,
    PPLine_FK INTEGER,
    Vessel_FK INTEGER,
    File_FK INTEGER,
    SailLine Text,
    Line INTEGER,
    Attempt TEXT,
    Seq INTEGER,
    Tier INTEGER DEFAULT 0,
    TierLinePoint INTEGER DEFAULT 0,
    LinePoint INTEGER DEFAULT 0,
    PointIdx INTEGER,
    Point INTEGER DEFAULT 0,
    PointCode TEXT,
    FireCode TEXT,
    ArrayCode INTEGER,
    Static REAL DEFAULT 0,
    PointDepth REAL DEFAULT 0,
    Datum INTEGER DEFAULT 0,
    Uphole REAL DEFAULT 0,
    WaterDepth INTEGER DEFAULT 0,
    Easting REAL DEFAULT 0,
    Northing REAL DEFAULT 0,
    Elevation REAL DEFAULT 0,
    JDay INTEGER DEFAULT 0,
    Hour INTEGER DEFAULT 0,
    Minute INTEGER DEFAULT 0,
    Second INTEGER DEFAULT 0,
    Microsecond REAL DEFAULT 0,
    Month INTEGER DEFAULT 0,
    Week INTEGER DEFAULT 0,
    Day INTEGER DEFAULT 0,
    Year INTEGER DEFAULT 0,
    YearDay TEXT,
    TimeStamp DATETIME,
    Vessel TEXT,
    RadialOffset REAL DEFAULT 0,
    ILOffset REAL DEFAULT 0,
    XLOffset REAL DEFAULT 0,
    isCompared INTEGER DEFAULT 0,
    isInSpec INTEGER DEFAULT 0,
    isILInSpec INTEGER DEFAULT 0,
    isXLInSpec INTEGER DEFAULT 0,
    PP_X REAL DEFAULT 0,
    PP_Y REAL DEFAULT 0,
    dX REAL DEFAULT 0,
    dY REAL DEFAULT 0,
    isPreplotCompared INTEGER DEFAULT 0,
    Spare1 INTEGER DEFAULT 0,
    Spare2 INTEGER DEFAULT 0,
    Spare3 INTEGER DEFAULT 0,
    FOREIGN KEY (SailLine_FK) REFERENCES SLSolution(ID) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (PPLine_FK) REFERENCES SLPreplot(ID)  ON UPDATE CASCADE,
    FOREIGN KEY (File_FK) REFERENCES Files(ID) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (Vessel_FK) REFERENCES project_fleet(ID) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_sps_file_line_ts
ON SPSolution (File_FK, SailLine_FK, TimeStamp);

CREATE INDEX IF NOT EXISTS idx_sps_line_ts
ON SPSolution (SailLine_FK, TimeStamp);

CREATE INDEX IF NOT EXISTS idx_sps_line_fire_ts
ON SPSolution (SailLine_FK, FireCode, TimeStamp);

CREATE INDEX IF NOT EXISTS idx_sl_file
ON SLSolution (File_FK);
-- For FK joins (always good)
CREATE INDEX IF NOT EXISTS idx_slsolution_file
ON SLSolution (File_FK);

CREATE INDEX IF NOT EXISTS idx_slsolution_ppline
ON SLSolution (PPLine_FK);

-- For your “group by / filter by line meta”
CREATE INDEX IF NOT EXISTS idx_slsolution_line_seq_attempt
ON SLSolution (Line, Seq, Attempt);

-- For TierLine queries
CREATE INDEX IF NOT EXISTS idx_slsolution_tierline
ON SLSolution (TierLine);

-- If you filter by vessel often
CREATE INDEX IF NOT EXISTS idx_slsolution_vessel
ON SLSolution (Vessel_FK);

-- Most queries: points of a line
CREATE INDEX IF NOT EXISTS idx_spsolution_line_point
ON SPSolution (SailLine_FK, Point);

-- If you use PointIdx as well
CREATE INDEX IF NOT EXISTS idx_spsolution_line_pointidx
ON SPSolution (SailLine_FK, PointIdx);

-- Fire code stats per line
CREATE INDEX IF NOT EXISTS idx_spsolution_line_firecode
ON SPSolution (SailLine_FK, FireCode);

-- File-based filtering
CREATE INDEX IF NOT EXISTS idx_spsolution_file
ON SPSolution (File_FK);

-- PPLine join (if you map to preplot line often)
CREATE INDEX IF NOT EXISTS idx_spsolution_ppline
ON SPSolution (PPLine_FK);

--Create Receiver Solution
CREATE TABLE  IF NOT EXISTS  RLSolution (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        PPLine_FK INTEGER,
        File_FK INTEGER,
        LineName TEXT,
        Line INTEGER,
        LineSolution INTEGER NOT NULL,
        Seq INTEGER DEFAULT 1,
        Attempt TEXT,
        Tier INTEGER,
        TierLine INTEGER,
        FRP INTEGER,
        LRP INTEGER,
        StartX REAL DEFAULT 0,
        StartY REAL DEFAULT 0,
        EndX REAL DEFAULT 0,
        EndY REAL DEFAULT 0,
        SRP INTEGER,
        ERP INTEGER,
        Vessel Text,
        StartYear INTEGER,
        StartMonth INTEGER,
        StartJDay INTEGER,
        StartDay INTEGER,
        StartHour INTEGER,
        StartMinute INTEGER,
        StartSecond INTEGER,
        StartMSecond REAL,
        EndYear INTEGER,
        EndMonth INTEGER,
        EndJDay INTEGER,
        EndDay INTEGER,
        EndHour INTEGER,
        EndMinute INTEGER,
        EndSecond REAL,
        EndMSecond REAL,
        Solution_FK INTEGER,
        PercentOfLineDone REAL,
        SeqProdCount REAL,
        PercentOFSeqDone REAL,
        Count_All INTEGER DEFAULT 0,
        is_clicked INTEGER DEFAULT 0,
        is_recovered INTEGER DEFAULT 0,
        is_fbloaded INTEGER DEFAULT 0,
        FileName_FK INTEGER,
        FOREIGN KEY (PPLine_FK) REFERENCES RLPreplot(ID) ON DELETE CASCADE,
        FOREIGN KEY (Solution_FK) REFERENCES Solutions(ID) ON DELETE CASCADE,
        FOREIGN KEY (FileName_FK) REFERENCES Files(ID) ON DELETE CASCADE,
        UNIQUE(LineSolution),
        UNIQUE(ID,LineSolution));
CREATE TABLE  IF NOT EXISTS  RPSolution (
       	ID INTEGER PRIMARY KEY AUTOINCREMENT,
       	LineName_FK	INTEGER,
       	Line INTEGER,
       	PP_Point_FK INTEGER,
       	PP_Line_FK INTEGER,
       	File_FK INTEGER,
       	Solution_FK INTEGER,
	    Tier INTEGER DEFAULT 0,
	    TierLinePoint INTEGER DEFAULT 0,
	    LinePoint INTEGER DEFAULT 0,
	    LinePointIdx INTEGER DEFAULT 0,
	    LinePointIdxSol INTEGER NOT NULL,
	    Point INTEGER DEFAULT 0,
	    PointIdx INTEGER,
	    FireCode TEXT,
	    Seq INTEGER DEFAULT 1,
	    ArrayNumber	INTEGER,
	    FCodeIdx INTEGER DEFAULT 0,
	    PointCode TEXT,
	    Static REAL DEFAULT 0,
	    PointDepth REAL DEFAULT 0,
	    Datum INTEGER DEFAULT 0,
	    Uphole REAL DEFAULT 0,
	    WaterDepth INTEGER DEFAULT 0,
	    Easting REAL DEFAULT 0,
	    Northing REAL DEFAULT 0,
	    Elevation REAL DEFAULT 0,
	    JDay INTEGER DEFAULT 0,
	    Hour INTEGER DEFAULT 0,
	    Minute INTEGER DEFAULT 0,
	    Second INTEGER DEFAULT 0,
	    Msecond REAL DEFAULT 0,
	    Month INTEGER DEFAULT 0,
	    Week INTEGER DEFAULT 0,
	    Day INTEGER DEFAULT 0,
	    Year INTEGER DEFAULT 0,
	    TimeStamp TEXT,
	    Date DATETIME,
	    YearDay TEXT,
		Vessel TEXT,
	    RadialOffset REAL DEFAULT 0,
	    ILOffset REAL DEFAULT 0,
	    XLOffset REAL DEFAULT 0,
	    isCompared	INTEGER DEFAULT 0,
	    isInSpec	INTEGER DEFAULT 0,
	    isILInSpec	INTEGER DEFAULT 0,
	    isXLInSpec	INTEGER DEFAULT 0,
	    PP_X REAL DEFAULT 0,
		PP_Y REAL DEFAULT 0,
	    dX REAL DEFAULT 0,
	    dY REAL DEFAULT 0,
	    isPreplotCompared INTEGER DEFAULT 0,
	    NODE_ID TEXT,
        DEPLOY INTEGER DEFAULT 1,
        RPI INTEGER DEFAULT 1,
        REC_X REAL DEFAULT 0,
        REC_Y REAL DEFAULT 0,
        REC_Z REAL DEFAULT 0,

        NEARILIN INTEGER DEFAULT 0,
        NEARXLIN INTEGER DEFAULT 0,
        TIMECORR  REAL DEFAULT 0,
        BULKSHIFT REAL DEFAULT 0,
        TIMINGEQ REAL DEFAULT 0,
        QDRIFT REAL DEFAULT 0,
        LDRIFT REAL DEFAULT 0,
        TRIMPTCH REAL DEFAULT 0,
        TRIMROLL REAL DEFAULT 0,
        TRIMYAW REAL DEFAULT 0,
        PITCHFIN REAL DEFAULT 0,
        ROLLFIN REAL DEFAULT 0,
        YAWFIN REAL DEFAULT 0,
        TOTDAYS REAL DEFAULT 0,
        NODSTART INTEGER DEFAULT 0,
        DEPLOYTM INTEGER DEFAULT 0,
        PICKUPTM INTEGER DEFAULT 0,
        RUNTIME  INTEGER DEFAULT 0,
        EC2_CD1 INTEGER DEFAULT 0,
        CLKFLAG INTEGER DEFAULT 0,
        EC1_RUS0 REAL DEFAULT 0,
        EC1_RUS1 REAL DEFAULT 0,
        EC1_EDT0 REAL DEFAULT 0,
        EC1_EDT1 REAL DEFAULT 0,
        EC1_EPT0 REAL DEFAULT 0,
        EC1_EPT1 REAL DEFAULT 0,
        TOTSHOTS INTEGER DEFAULT 0,
        TOTPROD INTEGER DEFAULT 0,
        SPSK INTEGER DEFAULT 0,
     	Spare1	INTEGER DEFAULT 0,
	    Spare2	INTEGER DEFAULT 0,
	    Spare3	INTEGER DEFAULT 0,
	    UNIQUE(LinePointIdxSol),
		UNIQUE(ID,LinePointIdxSol),
		FOREIGN KEY (LineName_FK) REFERENCES RLSolution(ID) ON DELETE CASCADE,
		FOREIGN KEY (PP_Point_FK) REFERENCES RPPreplot(ID) ON DELETE CASCADE,
		FOREIGN KEY (Solution_FK) REFERENCES Solutions(ID) ON DELETE CASCADE,
		FOREIGN KEY (PP_Line_FK) REFERENCES RLPreplot(ID) ON DELETE CASCADE,
		FOREIGN KEY (File_FK) REFERENCES Files(ID) ON DELETE CASCADE);
CREATE TABLE  IF NOT EXISTS  CSVLayers (
    ID INTEGER PRIMARY KEY,
    Name TEXT,
    Points INTEGER,
    Attr1Name TEXT,
    Attr2Name TEXT,
    Attr3Name TEXT,
    PointStyle TEXT DEFAULT 'circle',
    PointColor TEXT DEFAULT '#000000',
    PointSize INTEGER DEFAULT 1,
    Comments TEXT
);
CREATE TABLE  IF NOT EXISTS  CSVpoints (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Layer_FK INTEGER,
    Point TEXT,
    X REAL,
    Y REAL,
    Z REAL,
    Attr1 TEXT DEFAULT '',
    Attr2 INTEGER DEFAULT 0,
    Attr3 REAL DEFAULT 0,
    FOREIGN KEY (Layer_FK) REFERENCES CSVLayers(ID) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS project_shapes (
                                "id" INTEGER,
                                "FullName" TEXT UNIQUE NOT NULL,
                                "FileName" TEXT,
                                "isFilled" INTEGER DEFAULT 0,
                                "FillColor" TEXT DEFAULT '#000000',
                                "LineColor" TEXT DEFAULT '#000000',
                                "LineWidth" INTEGER DEFAULT 1,
                                "LineStyle" TEXT DEFAULT '',
                                "HatchPattern" TEXT DEFAULT '',
                                "FileCheck" INT DEFAULT 1,
	                            PRIMARY KEY(id,FullName));
CREATE TABLE IF NOT EXISTS project_fleet (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vessel_name TEXT NOT NULL,
    imo TEXT,
    mmsi TEXT,
    call_sign TEXT,
    vessel_type TEXT,
    owner TEXT,
    is_active INTEGER DEFAULT 1,
    is_retired INTEGER DEFAULT 0,
    notes TEXT,
    source_vessel_id INTEGER,         -- link back to Django Vessel.id
    created_at TEXT,
    updated_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_project_fleet_source_vessel_id
ON project_fleet(source_vessel_id);
CREATE TABLE IF NOT EXISTS "sequence_vessel_assignment" (
	"id"	INTEGER,
	"seq_first"	INTEGER NOT NULL,
	"seq_last"	INTEGER NOT NULL,
	"vessel_id"	INTEGER NOT NULL,
	"purpose"	TEXT,
	"purpose_id"	INTEGER NOT NULL DEFAULT 4,
	"comments"	TEXT,
	"is_active"	INTEGER DEFAULT 1,
	"created_at"	TEXT DEFAULT CURRENT_TIMESTAMP,
	"updated_at"	TEXT,
	PRIMARY KEY("id" AUTOINCREMENT),
	FOREIGN KEY("vessel_id") REFERENCES "project_fleet"("id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS SHOT_TABLE (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- H26 main fields
    sail_line INTEGER,
    shot_station INTEGER,
    shot_index INTEGER,
    shot_status INTEGER,

    -- Raw navigation code (LLLLLXSSSS)
    nav_line_code TEXT,

    -- Decoded navigation components
    nav_line INTEGER,
    attempt TEXT,
    seq INTEGER,

    -- Post point + extracted fire code
    post_point_code TEXT,
    fire_code TEXT,

    gun_depth REAL,
    water_depth REAL,

    shot_x REAL,
    shot_y REAL,

    shot_day INTEGER,
    shot_hour INTEGER,
    shot_minute INTEGER,
    shot_second INTEGER,
    shot_microsecond INTEGER,
    shot_year INTEGER,

    vessel TEXT,
    array_id TEXT,
    source_id INTEGER,

    nav_station INTEGER,
    shot_group_id INTEGER,
    elevation REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_shot_unique
ON SHOT_TABLE (
    nav_line_code,
    nav_station,
    post_point_code
);

DROP VIEW IF EXISTS PreplotSummaryAllFiles;
CREATE VIEW IF NOT EXISTS PreplotSummaryAllFiles AS

SELECT
  'RLPreplot' AS TableName,

  (SELECT COUNT(*) FROM RLPreplot) AS LinesCount,
  (SELECT SUM(COALESCE(Points,0)) FROM RLPreplot) AS TotalPoints,
  (SELECT ROUND(AVG(COALESCE(Points,0)),2) FROM RLPreplot) AS AvgPointsPerLine,
  (SELECT ROUND(AVG(COALESCE(RealLineLength,0)),2) FROM RLPreplot) AS AvgRealLineLength,

  (SELECT MIN(Line) FROM RLPreplot) AS MinLine,
  (SELECT MAX(Line) FROM RLPreplot) AS MaxLine,

  (SELECT COUNT(DISTINCT File_FK) FROM RLPreplot) AS FilesCount,

  (SELECT GROUP_CONCAT(FileName, '; ')
     FROM (
       SELECT DISTINCT COALESCE(f.FileName, 'NULL') AS FileName
       FROM RLPreplot r
       LEFT JOIN Files f ON f.ID = r.File_FK
       ORDER BY FileName
     )
  ) AS FileNames,

  (SELECT Line FROM RLPreplot ORDER BY COALESCE(Points,0) DESC, ID ASC LIMIT 1) AS LongestPointsLine,
  (SELECT COALESCE(Points,0) FROM RLPreplot ORDER BY COALESCE(Points,0) DESC, ID ASC LIMIT 1) AS LongestPoints,

  (SELECT Line FROM RLPreplot
     ORDER BY (COALESCE(Points,0)=0), COALESCE(Points,0) ASC, ID ASC
     LIMIT 1
  ) AS ShortestPointsLine,
  (SELECT COALESCE(Points,0) FROM RLPreplot
     ORDER BY (COALESCE(Points,0)=0), COALESCE(Points,0) ASC, ID ASC
     LIMIT 1
  ) AS ShortestPoints,

  (SELECT Line FROM RLPreplot ORDER BY COALESCE(RealLineLength,0) DESC, ID ASC LIMIT 1) AS LongestLengthLine,
  (SELECT COALESCE(RealLineLength,0) FROM RLPreplot ORDER BY COALESCE(RealLineLength,0) DESC, ID ASC LIMIT 1) AS LongestLength,

  (SELECT Line FROM RLPreplot
     ORDER BY (COALESCE(RealLineLength,0)=0), COALESCE(RealLineLength,0) ASC, ID ASC
     LIMIT 1
  ) AS ShortestLengthLine,
  (SELECT COALESCE(RealLineLength,0) FROM RLPreplot
     ORDER BY (COALESCE(RealLineLength,0)=0), COALESCE(RealLineLength,0) ASC, ID ASC
     LIMIT 1
  ) AS ShortestLength

UNION ALL

SELECT
  'SLPreplot' AS TableName,

  (SELECT COUNT(*) FROM SLPreplot) AS LinesCount,
  (SELECT SUM(COALESCE(Points,0)) FROM SLPreplot) AS TotalPoints,
  (SELECT ROUND(AVG(COALESCE(Points,0)),2) FROM SLPreplot) AS AvgPointsPerLine,
  (SELECT ROUND(AVG(COALESCE(RealLineLength,0)),2) FROM SLPreplot) AS AvgRealLineLength,

  (SELECT MIN(Line) FROM SLPreplot) AS MinLine,
  (SELECT MAX(Line) FROM SLPreplot) AS MaxLine,

  (SELECT COUNT(DISTINCT File_FK) FROM SLPreplot) AS FilesCount,

  (SELECT GROUP_CONCAT(FileName, '; ')
     FROM (
       SELECT DISTINCT COALESCE(f.FileName, 'NULL') AS FileName
       FROM SLPreplot s
       LEFT JOIN Files f ON f.ID = s.File_FK
       ORDER BY FileName
     )
  ) AS FileNames,

  (SELECT Line FROM SLPreplot ORDER BY COALESCE(Points,0) DESC, ID ASC LIMIT 1) AS LongestPointsLine,
  (SELECT COALESCE(Points,0) FROM SLPreplot ORDER BY COALESCE(Points,0) DESC, ID ASC LIMIT 1) AS LongestPoints,

  (SELECT Line FROM SLPreplot
     ORDER BY (COALESCE(Points,0)=0), COALESCE(Points,0) ASC, ID ASC
     LIMIT 1
  ) AS ShortestPointsLine,
  (SELECT COALESCE(Points,0) FROM SLPreplot
     ORDER BY (COALESCE(Points,0)=0), COALESCE(Points,0) ASC, ID ASC
     LIMIT 1
  ) AS ShortestPoints,

  (SELECT Line FROM SLPreplot ORDER BY COALESCE(RealLineLength,0) DESC, ID ASC LIMIT 1) AS LongestLengthLine,
  (SELECT COALESCE(RealLineLength,0) FROM SLPreplot ORDER BY COALESCE(RealLineLength,0) DESC, ID ASC LIMIT 1) AS LongestLength,

  (SELECT Line FROM SLPreplot
     ORDER BY (COALESCE(RealLineLength,0)=0), COALESCE(RealLineLength,0) ASC, ID ASC
     LIMIT 1
  ) AS ShortestLengthLine,
  (SELECT COALESCE(RealLineLength,0) FROM SLPreplot
     ORDER BY (COALESCE(RealLineLength,0)=0), COALESCE(RealLineLength,0) ASC, ID ASC
     LIMIT 1
  ) AS ShortestLength;
DROP VIEW IF EXISTS V_DSR_LineSummary;
CREATE VIEW IF NOT EXISTS V_DSR_LineSummary AS
WITH
rec_by_line AS (
    SELECT
        rl.Line AS Line,
        COUNT(*) AS ProcessedCount
    FROM REC_DB r
    JOIN RLPreplot rl
      ON rl.ID = r.Preplot_FK
    GROUP BY rl.Line
),

dsr_by_line AS (
    SELECT
        d.Line AS Line,

        -- Planned points
        MAX(rl.Points) AS PlannedPoints,

        -- RL line flags
        MAX(rl.isLineClicked)  AS isLineClicked,
        MAX(rl.isLineDeployed) AS isLineDeployed,
        MAX(rl.isValidated)    AS isValidated,

        -- Basic counts
        COUNT(*)                  AS DSRRows,
        COUNT(DISTINCT d.Station) AS Stations,
        COUNT(DISTINCT d.Node)    AS Nodes,

        MIN(d.Station) AS MinStation,
        MAX(d.Station) AS MaxStation,

        -- ROV Deployment / Retrieval counts
        SUM(CASE
            WHEN d.TimeStamp IS NOT NULL AND TRIM(d.TimeStamp) <> ''
            THEN 1 ELSE 0
        END) AS DeployedCount,

        SUM(CASE
            WHEN d.TimeStamp1 IS NOT NULL AND TRIM(d.TimeStamp1) <> ''
            THEN 1 ELSE 0
        END) AS RetrievedCount,

        -- SM flags
        SUM(CASE WHEN UPPER(TRIM(d.Deployed)) = 'YES' OR UPPER(TRIM(d.PickedUp)) = 'YES' THEN 1 ELSE 0 END) AS SMCount,
        SUM(CASE WHEN UPPER(TRIM(d.PickedUp)) = 'YES' THEN 1 ELSE 0 END) AS SMRCount,

        -- Timing (deployment)
        MIN(d.TimeStamp) AS FirstDeployTime,
        MAX(d.TimeStamp) AS LastDeployTime,
        ROUND((julianday(MAX(d.TimeStamp)) - julianday(MIN(d.TimeStamp))) * 24, 2) AS DeploymentHours,

        -- Timing (retrieval)
        MIN(d.TimeStamp1) AS FirstRetrieveTime,
        MAX(d.TimeStamp1) AS LastRetrieveTime,
        ROUND((julianday(MAX(d.TimeStamp1)) - julianday(MIN(d.TimeStamp1))) * 24, 2) AS RetrievalHours,

        -- Total operation time
        ROUND((julianday(MAX(d.TimeStamp1)) - julianday(MIN(d.TimeStamp))) * 24, 2) AS TotalOperationHours,

        -- Solution counts
        SUM(CASE WHEN d.Solution_FK = 1 THEN 1 ELSE 0 END) AS Normal,
        SUM(CASE WHEN d.Solution_FK = 2 THEN 1 ELSE 0 END) AS CoDeployed,
        SUM(CASE WHEN d.Solution_FK = 3 THEN 1 ELSE 0 END) AS Losted,
        SUM(CASE WHEN d.Solution_FK = 4 THEN 1 ELSE 0 END) AS Missplaced,
        SUM(CASE WHEN d.Solution_FK = 5 THEN 1 ELSE 0 END) AS WrongID,
        SUM(CASE WHEN d.Solution_FK = 6 THEN 1 ELSE 0 END) AS Overlap,

        -- Delta statistics
        AVG(d.DeltaEprimarytosecondary)  AS AvgDeltaE,
        MIN(d.DeltaEprimarytosecondary)  AS MinDeltaE,
        MAX(d.DeltaEprimarytosecondary)  AS MaxDeltaE,

        AVG(d.DeltaNprimarytosecondary)  AS AvgDeltaN,
        MIN(d.DeltaNprimarytosecondary)  AS MinDeltaN,
        MAX(d.DeltaNprimarytosecondary)  AS MaxDeltaN,

        AVG(d.DeltaEprimarytosecondary1) AS AvgDeltaE1,
        MIN(d.DeltaEprimarytosecondary1) AS MinDeltaE1,
        MAX(d.DeltaEprimarytosecondary1) AS MaxDeltaE1,

        AVG(d.DeltaNprimarytosecondary1) AS AvgDeltaN1,
        MIN(d.DeltaNprimarytosecondary1) AS MinDeltaN1,
        MAX(d.DeltaNprimarytosecondary1) AS MaxDeltaN1,

        -- Sigma statistics (ONLY Sigma..Sigma3)
        AVG(d.Sigma)  AS AvgSigma,
        MIN(d.Sigma)  AS MinSigma,
        MAX(d.Sigma)  AS MaxSigma,

        AVG(d.Sigma1) AS AvgSigma1,
        MIN(d.Sigma1) AS MinSigma1,
        MAX(d.Sigma1) AS MaxSigma1,

        AVG(d.Sigma2) AS AvgSigma2,
        MIN(d.Sigma2) AS MinSigma2,
        MAX(d.Sigma2) AS MaxSigma2,

        AVG(d.Sigma3) AS AvgSigma3,
        MIN(d.Sigma3) AS MinSigma3,
        MAX(d.Sigma3) AS MaxSigma3,

        -- Radial Offset (RangeToPreplot) statistics
        AVG(d.RangetoPrePlot) AS AvgRadOffset,
        MIN(d.RangetoPrePlot) AS MinRadOffset,
        MAX(d.RangetoPrePlot) AS MaxRadOffset,

        -- Range Primary to Secondary statistics
        AVG(d.Rangeprimarytosecondary) AS AvgRangePrimToSec,
        MIN(d.Rangeprimarytosecondary) AS MinRangePrimToSec,
        MAX(d.Rangeprimarytosecondary) AS MaxRangePrimToSec,

        -- Elevation stats
        AVG(d.PrimaryElevation)   AS AvgPrimaryElevation,
        MIN(d.PrimaryElevation)   AS MinPrimaryElevation,
        MAX(d.PrimaryElevation)   AS MaxPrimaryElevation,

        AVG(d.SecondaryElevation) AS AvgSecondaryElevation,
        MIN(d.SecondaryElevation) AS MinSecondaryElevation,
        MAX(d.SecondaryElevation) AS MaxSecondaryElevation

    FROM DSR d
    LEFT JOIN RLPreplot rl
      ON rl.Line = d.Line
    GROUP BY d.Line
),

-- ✅ NEW: exactly one config per line (deterministic pick)
config_per_line AS (
    SELECT
        d.Line,
        MIN(bcl.ID) AS ConfigID
    FROM DSR d
    JOIN BBox_Configs_List bcl
      ON TRIM(d.ROV) = bcl.rov1_name
      OR TRIM(d.ROV) = bcl.rov2_name
    WHERE d.ROV IS NOT NULL AND TRIM(d.ROV) <> ''
    GROUP BY d.Line
)

SELECT
    s.Line,
    s.PlannedPoints,

    s.isLineClicked,
    s.isLineDeployed,
    s.isValidated,

    s.DSRRows,
    s.Stations,
    s.Nodes,
    s.MinStation,
    s.MaxStation,

    s.DeployedCount,
    s.RetrievedCount,

    s.SMCount,
    s.SMRCount,

    COALESCE(r.ProcessedCount, 0) AS ProcessedCount,

    s.FirstDeployTime,
    s.LastDeployTime,
    s.DeploymentHours,

    s.FirstRetrieveTime,
    s.LastRetrieveTime,
    s.RetrievalHours,

    s.TotalOperationHours,

    ROUND(100.0 * s.DeployedCount / NULLIF(s.PlannedPoints, 0), 1) AS DeployedPct,
    ROUND(100.0 * s.RetrievedCount / NULLIF(s.PlannedPoints, 0), 1) AS RetrievedPct,
    ROUND(100.0 * COALESCE(r.ProcessedCount, 0) / NULLIF(s.PlannedPoints, 0), 1) AS ProcessedPct,

    s.Normal,
    s.CoDeployed,
    s.Losted,
    s.Missplaced,
    s.WrongID,
    s.Overlap,

    s.AvgDeltaE,  s.MinDeltaE,  s.MaxDeltaE,
    s.AvgDeltaN,  s.MinDeltaN,  s.MaxDeltaN,
    s.AvgDeltaE1, s.MinDeltaE1, s.MaxDeltaE1,
    s.AvgDeltaN1, s.MinDeltaN1, s.MaxDeltaN1,

    s.AvgSigma,  s.MinSigma,  s.MaxSigma,
    s.AvgSigma1, s.MinSigma1, s.MaxSigma1,
    s.AvgSigma2, s.MinSigma2, s.MaxSigma2,
    s.AvgSigma3, s.MinSigma3, s.MaxSigma3,

    -- 95% ellipse semi-axes using chi-square 0.95, df=2 (sqrt(5.991)=2.44774683068)
    (s.MaxSigma  * 2.44774683068) AS Primary_e95,
    (s.MaxSigma1 * 2.44774683068) AS Primary_n95,

    -- NEW outputs
    s.AvgRadOffset, s.MinRadOffset, s.MaxRadOffset,
    s.AvgRangePrimToSec, s.MinRangePrimToSec, s.MaxRangePrimToSec,

    -- ✅ Config fields (one config per line)
    bcl.*

FROM dsr_by_line s
LEFT JOIN rec_by_line r
  ON r.Line = s.Line
LEFT JOIN config_per_line cpl
  ON cpl.Line = s.Line
LEFT JOIN BBox_Configs_List bcl
  ON bcl.ID = cpl.ConfigID;
DROP VIEW IF EXISTS DEPLOY_ROV_Summary;
CREATE VIEW IF NOT EXISTS DEPLOY_ROV_Summary AS
WITH base AS (
    SELECT
        NULLIF(TRIM(ROV),  '') AS Rov,
        NULLIF(TRIM(ROV1), '') AS Rov1,
        TRIM(Line)        AS Line,
        TRIM(LinePoint)   AS LinePoint,
        TRIM(Status)      AS Status,
        TRIM(TimeStamp)   AS TS,
        TRIM(TimeStamp1)  AS TS1
    FROM DSR
),

norm AS (
    SELECT
        COALESCE(Rov, Rov1) AS RovKey,
        Rov, Rov1, Line, LinePoint, Status,

        -- Deploy day
        CASE
            WHEN TS IS NULL OR TS = '' THEN NULL
            WHEN length(TS) >= 10 THEN date(substr(TS, 1, 10))
            ELSE NULL
        END AS Day,

        -- Recovery day
        CASE
            WHEN TS1 IS NULL OR TS1 = '' THEN NULL
            WHEN length(TS1) >= 10 THEN date(substr(TS1, 1, 10))
            ELSE NULL
        END AS Day1
    FROM base
),

agg AS (
    SELECT
        RovKey AS Rov,

        /* ===== DEPLOY ===== */
        COUNT(DISTINCT CASE WHEN Rov IS NOT NULL THEN Line END)       AS Lines,
        COUNT(DISTINCT CASE WHEN Rov IS NOT NULL THEN LinePoint END)  AS Stations,
        COUNT(CASE           WHEN Rov IS NOT NULL THEN 1 END)         AS Nodes,
        COUNT(DISTINCT CASE  WHEN Rov IS NOT NULL THEN Day END)       AS Days,
        MIN(CASE WHEN Rov IS NOT NULL THEN Day END)                   AS FirstDay,
        MAX(CASE WHEN Rov IS NOT NULL THEN Day END)                   AS LastDay,

        /* ===== RECOVERY ===== */
        COUNT(DISTINCT CASE WHEN Rov1 IS NOT NULL AND Day1 IS NOT NULL THEN Line END)       AS RECLines,
        COUNT(DISTINCT CASE WHEN Rov1 IS NOT NULL AND Day1 IS NOT NULL THEN LinePoint END)  AS RECStations,
        COUNT(CASE           WHEN Rov1 IS NOT NULL AND Day1 IS NOT NULL THEN 1 END)         AS RECNodes,
        COUNT(DISTINCT CASE  WHEN Rov1 IS NOT NULL AND Day1 IS NOT NULL THEN Day1 END)      AS RECDays,
        MIN(CASE WHEN Rov1 IS NOT NULL AND Day1 IS NOT NULL THEN Day1 END)                  AS RECFirstDay,
        MAX(CASE WHEN Rov1 IS NOT NULL AND Day1 IS NOT NULL THEN Day1 END)                  AS RECLastDay,

        /* ===== STATUS COUNTS (Deploy side) ===== */
        COUNT(DISTINCT CASE WHEN Rov IS NOT NULL AND Status='Collected' THEN Line END)       AS ColLines,
        COUNT(DISTINCT CASE WHEN Rov IS NOT NULL AND Status='Collected' THEN LinePoint END)  AS ColStations,
        COUNT(CASE           WHEN Rov IS NOT NULL AND Status='Collected' THEN 1 END)         AS ColNodes,

        COUNT(DISTINCT CASE WHEN Rov IS NOT NULL AND Status='Picked Up' THEN Line END)       AS PULines,
        COUNT(DISTINCT CASE WHEN Rov IS NOT NULL AND Status='Picked Up' THEN LinePoint END)  AS PUStations,
        COUNT(CASE           WHEN Rov IS NOT NULL AND Status='Picked Up' THEN 1 END)         AS PUNodes,

        COUNT(DISTINCT CASE WHEN Rov IS NOT NULL AND Status='Deployed' THEN Line END)        AS SMDLines,
        COUNT(DISTINCT CASE WHEN Rov IS NOT NULL AND Status='Deployed' THEN LinePoint END)   AS SMDStations,
        COUNT(CASE           WHEN Rov IS NOT NULL AND Status='Deployed' THEN 1 END)          AS SMDNodes

    FROM norm
    WHERE RovKey IS NOT NULL
    GROUP BY RovKey
)

-- Per ROV rows
SELECT * FROM agg

UNION ALL

-- TOTAL row
SELECT
    'Total' AS Rov,

    SUM(Lines)     AS Lines,
    SUM(Stations)  AS Stations,
    SUM(Nodes)     AS Nodes,

    -- Deploy Days = span from earliest FirstDay to latest LastDay
    CASE WHEN MIN(FirstDay) IS NOT NULL AND MAX(LastDay) IS NOT NULL
         THEN CAST(julianday(MAX(LastDay)) - julianday(MIN(FirstDay)) + 1 AS INT)
         ELSE NULL
    END AS Days,
    MIN(FirstDay)  AS FirstDay,
    MAX(LastDay)   AS LastDay,

    SUM(RECLines)     AS RECLines,
    SUM(RECStations)  AS RECStations,
    SUM(RECNodes)     AS RECNodes,

    -- Recovery Days span
    CASE WHEN MIN(RECFirstDay) IS NOT NULL AND MAX(RECLastDay) IS NOT NULL
         THEN CAST(julianday(MAX(RECLastDay)) - julianday(MIN(RECFirstDay)) + 1 AS INT)
         ELSE NULL
    END AS RECDays,
    MIN(RECFirstDay)  AS RECFirstDay,
    MAX(RECLastDay)   AS RECLastDay,

    SUM(ColLines)     AS ColLines,
    SUM(ColStations)  AS ColStations,
    SUM(ColNodes)     AS ColNodes,

    SUM(PULines)      AS PULines,
    SUM(PUStations)   AS PUStations,
    SUM(PUNodes)      AS PUNodes,

    SUM(SMDLines)     AS SMDLines,
    SUM(SMDStations)  AS SMDStations,
    SUM(SMDNodes)     AS SMDNodes

FROM agg;

DROP VIEW IF EXISTS Daily_Deployment;
CREATE VIEW IF NOT EXISTS Daily_Deployment AS
SELECT
    DATE(TimeStamp)                 AS ProdDate,
    TRIM(Line)                      AS Line,
    TRIM(ROV)                       AS ROV,
    MIN(CAST(NULLIF(Station,'') AS REAL)) AS FRP,
    MAX(CAST(NULLIF(Station,'') AS REAL)) AS LRP,
    COUNT(*)                        AS TotalNodes
FROM DSR
WHERE TimeStamp IS NOT NULL
  AND TRIM(TimeStamp) <> ''
  AND ROV IS NOT NULL
  AND TRIM(ROV) <> ''
GROUP BY
    DATE(TimeStamp),
    TRIM(Line),
    TRIM(ROV);
DROP VIEW IF EXISTS Daily_Recovery;
CREATE VIEW IF NOT EXISTS Daily_Recovery AS
SELECT
    DATE(TimeStamp1)                AS ProdDate,
    TRIM(Line)                      AS Line,
    TRIM(ROV1)                      AS ROV,
    MIN(CAST(NULLIF(Station,'') AS REAL)) AS FRP,
    MAX(CAST(NULLIF(Station,'') AS REAL)) AS LRP,
    COUNT(*)                        AS TotalNodes
FROM DSR
WHERE TimeStamp IS NOT NULL
  AND TRIM(TimeStamp) <> ''
  AND ROV1 IS NOT NULL
  AND TRIM(ROV1) <> ''
GROUP BY
    DATE(TimeStamp),
    TRIM(Line),
    TRIM(ROV1);
DROP VIEW IF EXISTS V_SHOT_TABLE_SUMMARY;
CREATE VIEW IF NOT EXISTS V_SHOT_TABLE_SUMMARY AS
SELECT
    s.nav_line,
    s.attempt,
    s.seq,

    COUNT(s.nav_station) AS nav_station_count,
    COUNT(DISTINCT s.nav_station) AS nav_station_distinct_count,

    COUNT(DISTINCT s.source_id) AS SourceCount,
    COUNT(DISTINCT s.array_id) AS ArraysCount,

    MIN(s.gun_depth) AS min_gun_depth,
    MAX(s.gun_depth) AS max_gun_depth,

    MIN(s.water_depth) AS min_water_depth,
    MAX(s.water_depth) AS max_water_depth,

    -- Production count
    COUNT(DISTINCT CASE
        WHEN s.fire_code IS NOT NULL
         AND instr(pg.production_code, s.fire_code) > 0
        THEN s.nav_station
    END) AS ProdCount,

    -- Non-production count
    COUNT(DISTINCT CASE
        WHEN s.fire_code IS NOT NULL
         AND instr(pg.non_production_code, s.fire_code) > 0
        THEN s.nav_station
    END) AS NonProdCount,
    COUNT(DISTINCT CASE
        WHEN s.fire_code IS NOT NULL
         AND instr(pg.kill_code, s.fire_code) > 0
        THEN s.nav_station
    END) AS KillCount,

    -- Production percentage
    ROUND(
        100.0 *
        COUNT(DISTINCT CASE
            WHEN s.fire_code IS NOT NULL
             AND instr(pg.production_code, s.fire_code) > 0
            THEN s.nav_station
        END)
        / NULLIF(COUNT(DISTINCT s.nav_station), 0)
    , 2) AS ProdPercent,

    -- Non-production percentage
    ROUND(
        100.0 *
        COUNT(DISTINCT CASE
            WHEN s.fire_code IS NOT NULL
             AND instr(pg.non_production_code, s.fire_code) > 0
            THEN s.nav_station
        END)
        / NULLIF(COUNT(DISTINCT s.nav_station), 0)
    , 2) AS NonProdPercent,
    -- Kill points percentage
    ROUND(
        100.0 *
        COUNT(DISTINCT CASE
            WHEN s.fire_code IS NOT NULL
             AND instr(pg.kill_code, s.fire_code) > 0
            THEN s.nav_station
        END)
        / NULLIF(COUNT(DISTINCT s.nav_station), 0)
    , 2) AS KillPercent

FROM SHOT_TABLE s
CROSS JOIN project_geometry pg

GROUP BY
    s.nav_line,
    s.attempt,
    s.seq

ORDER BY
    s.nav_line,
    s.attempt,
    s.seq;
DROP VIEW IF EXISTS V_SLSolution_VesselPurposeSummary;
CREATE VIEW IF NOT EXISTS V_SLSolution_VesselPurposeSummary AS
SELECT
    s.Vessel_FK,
    pf.vessel_name,

    sva.purpose,
    sva.purpose_id,

    COUNT(*) AS SailLines,

    COUNT(DISTINCT s.Line) AS DistinctLines,
    COUNT(DISTINCT s.Seq)  AS DistinctSeqs,

    COUNT(DISTINCT printf('%d|%d',
        COALESCE(s.Line,-1),
        COALESCE(s.Seq,-1)
    )) AS DistinctLineSeq,

    SUM(COALESCE(s.ProductionCount,0))    AS ProductionTotal,
    SUM(COALESCE(s.NonProductionCount,0)) AS NonProductionTotal,
    SUM(COALESCE(s.KillCount,0))          AS KillTotal,

    MIN(s.Start_Time) AS FirstStartTime,
    MAX(s.End_Time)   AS LastEndTime,

    SUM(COALESCE(s.LineLength,0)) AS LineLengthTotal,

    MAX(COALESCE(s.MaxSPI,0)) AS MaxSPI,

    MIN(NULLIF(s.MinProdGunDepth,0))   AS MinProdGunDepth,
    MAX(NULLIF(s.MaxProdGunDepth,0))   AS MaxProdGunDepth,

    MIN(NULLIF(s.MinProdWaterDepth,0)) AS MinProdWaterDepth,
    MAX(NULLIF(s.MaxProdWaterDepth,0)) AS MaxProdWaterDepth

FROM SLSolution s

LEFT JOIN project_fleet pf
    ON pf.id = s.Vessel_FK

LEFT JOIN sequence_vessel_assignment sva
    ON sva.vessel_id = s.Vessel_FK
   AND s.Seq BETWEEN sva.seq_first AND sva.seq_last
   AND sva.is_active = 1

GROUP BY
    s.Vessel_FK,
    pf.vessel_name,
    sva.purpose,
    sva.purpose_id

ORDER BY
    pf.vessel_name,
    sva.purpose_id;
DROP VIEW IF EXISTS V_SHOT_LineSummary;

CREATE VIEW IF NOT EXISTS V_SHOT_LineSummary AS
WITH
pg AS (
    SELECT
        UPPER(COALESCE(production_code,''))      AS prod_codes,
        UPPER(COALESCE(non_production_code,''))  AS nonprod_codes,
        UPPER(COALESCE(kill_code,''))            AS kill_codes
    FROM project_geometry
    LIMIT 1
),

-- ===============================
-- SHOT base
-- ===============================
shot_base AS (
    SELECT
        s.nav_line_code,
        s.nav_line,
        s.attempt,
        s.seq,

        s.shot_station,
        s.shot_index,
        s.shot_status,

        s.post_point_code,
        s.fire_code,

        s.gun_depth,
        s.water_depth,

        s.shot_x,
        s.shot_y,

        s.shot_day,
        s.shot_hour,
        s.shot_minute,
        s.shot_second,
        s.shot_microsecond,
        s.shot_year,

        s.array_id,
        s.source_id,
        s.nav_station,
        s.shot_group_id,
        s.elevation,

        CASE WHEN INSTR(pg.prod_codes,    UPPER(COALESCE(s.fire_code,''))) > 0 THEN 1 ELSE 0 END AS is_prod,
        CASE WHEN INSTR(pg.nonprod_codes, UPPER(COALESCE(s.fire_code,''))) > 0 THEN 1 ELSE 0 END AS is_nonprod,
        CASE WHEN INSTR(pg.kill_codes,    UPPER(COALESCE(s.fire_code,''))) > 0 THEN 1 ELSE 0 END AS is_kill
    FROM SHOT_TABLE s
    CROSS JOIN pg
    WHERE s.nav_line_code IS NOT NULL
      AND TRIM(s.nav_line_code) <> ''
),

-- ===============================
-- SHOT aggregation
-- ===============================
shot_agg AS (
    SELECT
        nav_line_code,

        MAX(nav_line) AS nav_line,
        MAX(attempt)  AS attempt,
        MAX(seq)      AS seq,

        COUNT(*) AS ShotCount,

        SUM(is_prod)    AS ProdShots,
        SUM(is_nonprod) AS NonProdShots,
        SUM(is_kill)    AS KillShots,

        MIN(nav_station) AS FSP,
        MAX(nav_station) AS LSP,
        MIN(CASE WHEN is_prod=1 THEN nav_station END) AS FGSP,
        MAX(CASE WHEN is_prod=1 THEN nav_station END) AS LGSP,

        -- SHOT sums / checksums
        SUM(shot_station)     AS Sum_shot_station,
        SUM(shot_index)       AS Sum_shot_index,
        SUM(shot_status)      AS Sum_shot_status,

        SUM(attempt)          AS Sum_attempt,
        SUM(seq)              AS Sum_seq,

        SUM(unicode(post_point_code)) AS Sum_post_point_code,
        SUM(unicode(fire_code))       AS Sum_fire_code,

        SUM(gun_depth)        AS Sum_gun_depth,
        SUM(water_depth)      AS Sum_water_depth,

        SUM(shot_x)           AS Sum_shot_x,
        SUM(shot_y)           AS Sum_shot_y,

        SUM(shot_day)         AS Sum_shot_day,
        SUM(shot_hour)        AS Sum_shot_hour,
        SUM(shot_minute)      AS Sum_shot_minute,
        SUM(shot_second)      AS Sum_shot_second,
        SUM(shot_microsecond) AS Sum_shot_microsecond,
        SUM(shot_year)        AS Sum_shot_year,

        SUM(unicode(array_id)) AS Sum_array_id,

        SUM(source_id)     AS Sum_source_id,
        SUM(nav_station)   AS Sum_nav_station,
        SUM(shot_group_id) AS Sum_shot_group_id,
        SUM(elevation)     AS Sum_elevation

    FROM shot_base
    GROUP BY nav_line_code
),

-- ===============================
-- SPSolution aggregation
-- ===============================
sps_agg AS (
    SELECT
        SailLine,
        Line,
        Attempt,
        Seq,

        SUM(Line)    AS sps_Sum_Line,
        SUM(Attempt) AS sps_Sum_Attempt,
        SUM(Seq)     AS sps_Sum_Seq,
        SUM(Point)   AS sps_Sum_Point,

        SUM(unicode(PointCode)) AS sps_Sum_PointCode,
        SUM(unicode(FireCode))  AS sps_Sum_FireCode,
        SUM(unicode(ArrayCode)) AS sps_Sum_ArrayCode,

        SUM(Static)     AS sps_Sum_Static,
        SUM(PointDepth) AS sps_Sum_PointDepth,
        SUM(Datum)      AS sps_Sum_Datum,
        SUM(Uphole)     AS sps_Sum_Uphole,
        SUM(WaterDepth) AS sps_Sum_WaterDepth,

        SUM(Easting)   AS sps_Sum_Easting,
        SUM(Northing)  AS sps_Sum_Northing,
        SUM(Elevation) AS sps_Sum_Elevation,

        SUM(JDay)        AS sps_Sum_JDay,
        SUM(Hour)        AS sps_Sum_Hour,
        SUM(Minute)      AS sps_Sum_Minute,
        SUM(Second)      AS sps_Sum_Second,
        SUM(Microsecond) AS sps_Sum_Microsecond

    FROM SPSolution
    GROUP BY SailLine, Line, Attempt, Seq
)

-- ===============================
-- FINAL SELECT
-- ===============================
SELECT
    a.nav_line_code,
    a.nav_line,
    a.attempt,
    a.seq,

    sva.purpose,
    sva.vessel_id,
    pf.vessel_name,

    CASE WHEN EXISTS (
        SELECT 1
        FROM SLSolution sl
        WHERE sl.SailLine = a.nav_line_code
    ) THEN 1 ELSE 0 END AS IsInSLSolution,

    a.ShotCount,
    a.ProdShots,
    a.NonProdShots,
    a.KillShots,

    a.FSP,
    a.LSP,
    a.FGSP,
    a.LGSP,

    -- SHOT sums
    a.Sum_shot_station,
    a.Sum_shot_index,
    a.Sum_shot_status,
    a.Sum_attempt,
    a.Sum_seq,
    a.Sum_post_point_code,
    a.Sum_fire_code,
    a.Sum_gun_depth,
    a.Sum_water_depth,
    a.Sum_shot_x,
    a.Sum_shot_y,
    a.Sum_shot_day,
    a.Sum_shot_hour,
    a.Sum_shot_minute,
    a.Sum_shot_second,
    a.Sum_shot_microsecond,
    a.Sum_shot_year,
    a.Sum_array_id,
    a.Sum_source_id,
    a.Sum_nav_station,
    a.Sum_shot_group_id,
    a.Sum_elevation,

    -- SPS sums
    s.SailLine AS sps_SailLine,
    s.sps_Sum_Line,
    s.sps_Sum_Attempt,
    s.sps_Sum_Seq,
    s.sps_Sum_Point,
    s.sps_Sum_PointCode,
    s.sps_Sum_FireCode,
    s.sps_Sum_ArrayCode,
    s.sps_Sum_Static,
    s.sps_Sum_PointDepth,
    s.sps_Sum_Datum,
    s.sps_Sum_Uphole,
    s.sps_Sum_WaterDepth,
    s.sps_Sum_Easting,
    s.sps_Sum_Northing,
    s.sps_Sum_Elevation,
    s.sps_Sum_JDay,
    s.sps_Sum_Hour,
    s.sps_Sum_Minute,
    s.sps_Sum_Second,
    s.sps_Sum_Microsecond,

    -- ---------------------------------
    -- Comparisons (1=match, 0=mismatch)
    -- ArrayCode removed from flags
    -- ---------------------------------
    CASE WHEN COALESCE(a.nav_line,0)             = COALESCE(s.sps_Sum_Line, -999999999)        THEN 1 ELSE 0 END AS cmp_Line,
    CASE WHEN COALESCE(a.Sum_attempt,0)          = COALESCE(s.sps_Sum_Attempt, -999999999)     THEN 1 ELSE 0 END AS cmp_Attempt,
    CASE WHEN COALESCE(a.Sum_seq,0)              = COALESCE(s.sps_Sum_Seq, -999999999)         THEN 1 ELSE 0 END AS cmp_Seq,

    CASE WHEN COALESCE(a.Sum_nav_station,0)      = COALESCE(s.sps_Sum_Point, -999999999)       THEN 1 ELSE 0 END AS cmp_Point,

    CASE WHEN COALESCE(a.Sum_post_point_code,0)  = COALESCE(s.sps_Sum_PointCode, -999999999)   THEN 1 ELSE 0 END AS cmp_PointCode,
    CASE WHEN COALESCE(a.Sum_fire_code,0)        = COALESCE(s.sps_Sum_FireCode, -999999999)    THEN 1 ELSE 0 END AS cmp_FireCode,

    CASE WHEN COALESCE(a.Sum_water_depth,0)      = COALESCE(s.sps_Sum_WaterDepth, -999999999)  THEN 1 ELSE 0 END AS cmp_WaterDepth,
    CASE WHEN COALESCE(a.Sum_shot_x,0)           = COALESCE(s.sps_Sum_Easting, -999999999)     THEN 1 ELSE 0 END AS cmp_Easting,
    CASE WHEN COALESCE(a.Sum_shot_y,0)           = COALESCE(s.sps_Sum_Northing, -999999999)    THEN 1 ELSE 0 END AS cmp_Northing,
    CASE WHEN COALESCE(a.Sum_elevation,0)        = COALESCE(s.sps_Sum_Elevation, -999999999)   THEN 1 ELSE 0 END AS cmp_Elevation,

    CASE WHEN COALESCE(a.Sum_shot_day,0)         = COALESCE(s.sps_Sum_JDay, -999999999)        THEN 1 ELSE 0 END AS cmp_JDay,
    CASE WHEN COALESCE(a.Sum_shot_hour,0)        = COALESCE(s.sps_Sum_Hour, -999999999)        THEN 1 ELSE 0 END AS cmp_Hour,
    CASE WHEN COALESCE(a.Sum_shot_minute,0)      = COALESCE(s.sps_Sum_Minute, -999999999)      THEN 1 ELSE 0 END AS cmp_Minute,
    CASE WHEN COALESCE(a.Sum_shot_second,0)      = COALESCE(s.sps_Sum_Second, -999999999)      THEN 1 ELSE 0 END AS cmp_Second,
    CASE WHEN COALESCE(a.Sum_shot_microsecond,0) = COALESCE(s.sps_Sum_Microsecond, -999999999) THEN 1 ELSE 0 END AS cmp_Microsecond,

    -- diffs (SHOT - SPS), ArrayCode removed
    (COALESCE(a.Sum_attempt,0)          - COALESCE(s.sps_Sum_Attempt,0))      AS diff_Attempt,
    (COALESCE(a.Sum_seq,0)              - COALESCE(s.sps_Sum_Seq,0))          AS diff_Seq,
    (COALESCE(a.Sum_nav_station,0)      - COALESCE(s.sps_Sum_Point,0))        AS diff_Point,
    (COALESCE(a.Sum_post_point_code,0)  - COALESCE(s.sps_Sum_PointCode,0))    AS diff_PointCode,
    (COALESCE(a.Sum_fire_code,0)        - COALESCE(s.sps_Sum_FireCode,0))     AS diff_FireCode,
    (COALESCE(a.Sum_water_depth,0)      - COALESCE(s.sps_Sum_WaterDepth,0))   AS diff_WaterDepth,
    (COALESCE(a.Sum_shot_x,0)           - COALESCE(s.sps_Sum_Easting,0))      AS diff_Easting,
    (COALESCE(a.Sum_shot_y,0)           - COALESCE(s.sps_Sum_Northing,0))     AS diff_Northing,
    (COALESCE(a.Sum_elevation,0)        - COALESCE(s.sps_Sum_Elevation,0))    AS diff_Elevation,
    (COALESCE(a.Sum_shot_day,0)         - COALESCE(s.sps_Sum_JDay,0))         AS diff_JDay,
    (COALESCE(a.Sum_shot_hour,0)        - COALESCE(s.sps_Sum_Hour,0))         AS diff_Hour,
    (COALESCE(a.Sum_shot_minute,0)      - COALESCE(s.sps_Sum_Minute,0))       AS diff_Minute,
    (COALESCE(a.Sum_shot_second,0)      - COALESCE(s.sps_Sum_Second,0))       AS diff_Second,
    (COALESCE(a.Sum_shot_microsecond,0) - COALESCE(s.sps_Sum_Microsecond,0))  AS diff_Microsecond,

    -- Sum of absolute diffs (QC severity). 0 = perfect match.
    ABS(COALESCE(a.Sum_attempt,0)          - COALESCE(s.sps_Sum_Attempt,0)) +
    ABS(COALESCE(a.Sum_seq,0)              - COALESCE(s.sps_Sum_Seq,0)) +
    ABS(COALESCE(a.Sum_nav_station,0)      - COALESCE(s.sps_Sum_Point,0)) +
    ABS(COALESCE(a.Sum_post_point_code,0)  - COALESCE(s.sps_Sum_PointCode,0)) +
    ABS(COALESCE(a.Sum_fire_code,0)        - COALESCE(s.sps_Sum_FireCode,0)) +
    ABS(COALESCE(a.Sum_water_depth,0)      - COALESCE(s.sps_Sum_WaterDepth,0)) +
    ABS(COALESCE(a.Sum_shot_x,0)           - COALESCE(s.sps_Sum_Easting,0)) +
    ABS(COALESCE(a.Sum_shot_y,0)           - COALESCE(s.sps_Sum_Northing,0)) +
    ABS(COALESCE(a.Sum_elevation,0)        - COALESCE(s.sps_Sum_Elevation,0)) +
    ABS(COALESCE(a.Sum_shot_day,0)         - COALESCE(s.sps_Sum_JDay,0)) +
    ABS(COALESCE(a.Sum_shot_hour,0)        - COALESCE(s.sps_Sum_Hour,0)) +
    ABS(COALESCE(a.Sum_shot_minute,0)      - COALESCE(s.sps_Sum_Minute,0)) +
    ABS(COALESCE(a.Sum_shot_second,0)      - COALESCE(s.sps_Sum_Second,0)) +
    ABS(COALESCE(a.Sum_shot_microsecond,0) - COALESCE(s.sps_Sum_Microsecond,0))
    AS SumDiff,

    -- overall QC flag
    CASE WHEN
        (COALESCE(a.nav_line,0)             = COALESCE(s.sps_Sum_Line, -999999999)) AND
        (COALESCE(a.Sum_attempt,0)          = COALESCE(s.sps_Sum_Attempt, -999999999)) AND
        (COALESCE(a.Sum_seq,0)              = COALESCE(s.sps_Sum_Seq, -999999999)) AND
        (COALESCE(a.Sum_nav_station,0)      = COALESCE(s.sps_Sum_Point, -999999999)) AND
        (COALESCE(a.Sum_post_point_code,0)  = COALESCE(s.sps_Sum_PointCode, -999999999)) AND
        (COALESCE(a.Sum_fire_code,0)        = COALESCE(s.sps_Sum_FireCode, -999999999)) AND
        (COALESCE(a.Sum_water_depth,0)      = COALESCE(s.sps_Sum_WaterDepth, -999999999)) AND
        (COALESCE(a.Sum_shot_x,0)           = COALESCE(s.sps_Sum_Easting, -999999999)) AND
        (COALESCE(a.Sum_shot_y,0)           = COALESCE(s.sps_Sum_Northing, -999999999)) AND
        (COALESCE(a.Sum_elevation,0)        = COALESCE(s.sps_Sum_Elevation, -999999999)) AND
        (COALESCE(a.Sum_shot_day,0)         = COALESCE(s.sps_Sum_JDay, -999999999)) AND
        (COALESCE(a.Sum_shot_hour,0)        = COALESCE(s.sps_Sum_Hour, -999999999)) AND
        (COALESCE(a.Sum_shot_minute,0)      = COALESCE(s.sps_Sum_Minute, -999999999)) AND
        (COALESCE(a.Sum_shot_second,0)      = COALESCE(s.sps_Sum_Second, -999999999)) AND
        (COALESCE(a.Sum_shot_microsecond,0) = COALESCE(s.sps_Sum_Microsecond, -999999999))
    THEN 1 ELSE 0 END AS QC_AllMatch,

    -- any match flag
    CASE WHEN
         (COALESCE(a.nav_line,0)             = COALESCE(s.sps_Sum_Line, -999999999))
      OR (COALESCE(a.Sum_attempt,0)          = COALESCE(s.sps_Sum_Attempt, -999999999))
      OR (COALESCE(a.Sum_seq,0)              = COALESCE(s.sps_Sum_Seq, -999999999))
      OR (COALESCE(a.Sum_nav_station,0)      = COALESCE(s.sps_Sum_Point, -999999999))
      OR (COALESCE(a.Sum_post_point_code,0)  = COALESCE(s.sps_Sum_PointCode, -999999999))
      OR (COALESCE(a.Sum_fire_code,0)        = COALESCE(s.sps_Sum_FireCode, -999999999))
      OR (COALESCE(a.Sum_water_depth,0)      = COALESCE(s.sps_Sum_WaterDepth, -999999999))
      OR (COALESCE(a.Sum_shot_x,0)           = COALESCE(s.sps_Sum_Easting, -999999999))
      OR (COALESCE(a.Sum_shot_y,0)           = COALESCE(s.sps_Sum_Northing, -999999999))
      OR (COALESCE(a.Sum_elevation,0)        = COALESCE(s.sps_Sum_Elevation, -999999999))
      OR (COALESCE(a.Sum_shot_day,0)         = COALESCE(s.sps_Sum_JDay, -999999999))
      OR (COALESCE(a.Sum_shot_hour,0)        = COALESCE(s.sps_Sum_Hour, -999999999))
      OR (COALESCE(a.Sum_shot_minute,0)      = COALESCE(s.sps_Sum_Minute, -999999999))
      OR (COALESCE(a.Sum_shot_second,0)      = COALESCE(s.sps_Sum_Second, -999999999))
      OR (COALESCE(a.Sum_shot_microsecond,0) = COALESCE(s.sps_Sum_Microsecond, -999999999))
    THEN 1 ELSE 0 END AS QC_AnyMatch

FROM shot_agg a
LEFT JOIN sps_agg s
    ON s.Line    = a.nav_line
   AND s.Attempt = a.attempt
   AND s.Seq     = a.seq
LEFT JOIN sequence_vessel_assignment sva
    ON a.seq BETWEEN sva.seq_first AND sva.seq_last
LEFT JOIN project_fleet pf
    ON pf.id = sva.vessel_id;

CREATE TABLE IF NOT EXISTS REC_DB
(
    "ID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "REC_ID"   INTEGER,
    "NODE_ID"  INTEGER,
    "DEPLOY"   INTEGER,
    "RPI"      INTEGER,
    "PART_NO"  INTEGER,
    "RPRE_X"   REAL,
    "RPRE_Y"   REAL,
    "RFIELD_X" REAL,
    "RFIELD_Y" REAL,
    "RFIELD_Z" REAL,
    "REC_X"    REAL,
    "REC_Y"    REAL,
    "REC_Z"    REAL,
    "TIMECORR" REAL,
    "BULKSHFT" REAL,
    "QDRIFT"   REAL,
    "LDRIFT"   REAL,
    "TRIMPTCH" REAL,
    "TRIMROLL" REAL,
    "TRIMYAW"  REAL,
    "PITCHFIN" REAL,
    "ROLLFIN"  REAL,
    "YAWFIN"   REAL,
    "TOTDAYS"  REAL,
    "RECCOUNT" INTEGER,
    "CLKFLAG"  INTEGER,
    "EC1_RUS0" REAL    DEFAULT 0,
    "EC1_RUS1" REAL    DEFAULT 0,
    "EC1_EDT0" REAL    DEFAULT 0,
    "EC1_EDT1" REAL    DEFAULT 0,
    "EC1_EPT0" REAL    DEFAULT 0,
    "EC1_EPT1" REAL    DEFAULT 0,
    "NODSTART" INTEGER DEFAULT 0,
    "DEPLOYTM" INTEGER DEFAULT 0,
    "PICKUPTM" INTEGER DEFAULT 0,
    "RUNTIME"  INTEGER DEFAULT 0,
    "EC2_CD1"  INTEGER DEFAULT 0,
    "TOTSHOTS" INTEGER DEFAULT 0,
    "TOTPROD"  INTEGER DEFAULT 0,
    "SPSK"     INTEGER DEFAULT 0,
    "TIER"     INTEGER DEFAULT 1,
    UNIQUE (REC_ID, DEPLOY, RPI)
);


