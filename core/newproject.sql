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
        LineName TEXT UNIQUE,
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
        Vessel Text,
        Start_Time DATETIME,
        End_Time DATETIME,
        Start_Production_Time DATETIME,
        End_Production_Time DATETIME,
        PercentOfLineDone REAL,
        SeqProdCount REAL,
        PercentOfSeqDone REAL,
        Count_All INTEGER,
        Count_A INTEGER,
        Count_P INTEGER,
        Count_L INTEGER,
        Count_R INTEGER,
        Count_X INTEGER,
        Count_M INTEGER,
        Count_K INTEGER,
        Count_W INTEGER,
        Count_T INTEGER,
        FileName_FK INTEGER,
        FOREIGN KEY (PPLine_FK) REFERENCES SLPreplot(ID)  ON UPDATE CASCADE,
        FOREIGN KEY (FileName_FK) REFERENCES Files(ID) ON DELETE CASCADE ON UPDATE CASCADE,
        UNIQUE(LineName, ID));
CREATE TABLE  IF NOT EXISTS  SPSolution (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    LineName_FK INTEGER,
    PPLine_FK INTEGER,
    PP_Point_FK INTEGER,
    FileName_FK INTEGER,
    Tier INTEGER DEFAULT 0,
    TierLinePoint INTEGER DEFAULT 0,
    LinePoint INTEGER DEFAULT 0,
    Point INTEGER DEFAULT 0,
    PointIdx INTEGER,
    FireCode TEXT,
    ArrayNumber INTEGER,
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
    UNIQUE(ID),
    FOREIGN KEY (LineName_FK) REFERENCES SLSolution(ID) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (PPLine_FK) REFERENCES SLPreplot(ID)  ON UPDATE CASCADE,
    FOREIGN KEY (PP_Point_FK) REFERENCES SPPreplot(ID) ON UPDATE CASCADE,
    FOREIGN KEY (FileName_FK) REFERENCES Files(ID) ON DELETE CASCADE ON UPDATE CASCADE
);
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
        SUM(CASE WHEN UPPER(TRIM(d.Deployed)) = 'YES' THEN 1 ELSE 0 END) AS SMCount,
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
        AVG(d.DeltaEprimarytosecondary) AS AvgDeltaE,
        MIN(d.DeltaEprimarytosecondary) AS MinDeltaE,
        MAX(d.DeltaEprimarytosecondary) AS MaxDeltaE,

        AVG(d.DeltaNprimarytosecondary) AS AvgDeltaN,
        MIN(d.DeltaNprimarytosecondary) AS MinDeltaN,
        MAX(d.DeltaNprimarytosecondary) AS MaxDeltaN,

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

        -- NEW: Radial Offset (RangeToPreplot) statistics
        AVG(d.RangetoPrePlot) AS AvgRadOffset,
        MIN(d.RangetoPrePlot) AS MinRadOffset,
        MAX(d.RangetoPrePlot) AS MaxRadOffset,

        -- NEW: Range Primary to Secondary statistics
        AVG(d.Rangeprimarytosecondary) AS AvgRangePrimToSec,
        MIN(d.Rangeprimarytosecondary) AS MinRangePrimToSec,
        MAX(d.Rangeprimarytosecondary) AS MaxRangePrimToSec,

        AVG(d.PrimaryElevation) AS AvgPrimaryElevation,
        MIN(d.PrimaryElevation) AS MinPrimaryElevation,
        MAX(d.PrimaryElevation) AS MaxPrimaryElevation,

		AVG(d.SecondaryElevation) AS AvgPrimaryElevation,
        MIN(d.SecondaryElevation) AS MinPrimaryElevation,
        MAX(d.SecondaryElevation) AS MaxPrimaryElevation
    FROM DSR d
    LEFT JOIN RLPreplot rl
      ON rl.Line = d.Line
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

    -- NEW outputs
    s.AvgRadOffset, s.MinRadOffset, s.MaxRadOffset,
    s.AvgRangePrimToSec, s.MinRangePrimToSec, s.MaxRangePrimToSec

FROM dsr_by_line s
LEFT JOIN rec_by_line r
  ON r.Line = s.Line;
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


