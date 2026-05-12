Project: SeisWebLog
git:https://github.com/khdenis76/SeisWebLog2026_public
Stack: Django 5.2, Python 3.11, Bokeh, Matplotlib, SQLite,
Structure:
- core: contains main project classes and models
Applications:
- baseproject: Load preplots,shapes, templates and csv layers
- rov: Work with ROVs load DSR, BBOX, REC_DB export SM and SPS files
- source: Load Source SPS and Shot Tables
- svp: Load sound velocity profiles
- noar: node on a roap shalow water nodes load SPS csv xlsx export to SPS 
- heavy plotting (Bokeh interactive, Matplotlib for reports,Generating tif with GDAL)
- database: custom SQLite schemas (DSR, REC_DB, SHOT_TABLE, etc.)
Applications:
- rov: work with DSR, BBOX files main application for work with Node deployment/recovery 
- source: work with Source SPS files production and non-production  and also with shot table
Coding rules:
- imports at top of the file. 
- Write detailed comments for functions 
- from utils.decorators import log_action add to all view files at top
- from core.models import UserSettings add to the top of view files
- from core.projectdb import ProjectDB add to the top of view files
- Bootstrap 5.3 UI
- prefer lazy loading via JS init functions
- use Bokeh JSON embedding for plots
- each bokeh plot should have option to is_show=False by default and can be able to save plot into html file 
Projec Data Base list:
D:\SeisWebLog2026_develop\myenv\Scripts\python.exe D:\SeisWebLog2026_develop\tests\db_tables_list.py 

================================================================================
TABLE: BBox_Config

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  FieldName                      TEXT            NOT NULL=True PK=False DEFAULT=None
  FileColumn                     TEXT            NOT NULL=False PK=False DEFAULT=None
  inUse                          INTEGER         NOT NULL=False PK=False DEFAULT=0
  CONFIG_FK                      INTEGER         NOT NULL=True PK=False DEFAULT=None

FOREIGN KEYS:
  CONFIG_FK -> BBox_Configs_List.ID ON UPDATE CASCADE ON DELETE RESTRICT

================================================================================
TABLE: BBox_Configs_List

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  Name                           TEXT            NOT NULL=True PK=False DEFAULT=None
  IsDefault                      INTEGER         NOT NULL=False PK=False DEFAULT=0
  rov1_name                      TEXT            NOT NULL=False PK=False DEFAULT=None
  rov2_name                      TEXT            NOT NULL=False PK=False DEFAULT=None
  gnss1_name                     TEXT            NOT NULL=False PK=False DEFAULT=None
  gnss2_name                     TEXT            NOT NULL=False PK=False DEFAULT=None
  Vessel_name                    TEXT            NOT NULL=False PK=False DEFAULT=None
  Depth1_name                    TEXT            NOT NULL=False PK=False DEFAULT=None
  Depth2_name                    TEXT            NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: BlackBox

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  TimeStamp                      TEXT            NOT NULL=False PK=False DEFAULT=None
  VesselEasting                  REAL            NOT NULL=False PK=False DEFAULT=None
  VesselNorthing                 REAL            NOT NULL=False PK=False DEFAULT=None
  VesselElevation                REAL            NOT NULL=False PK=False DEFAULT=None
  VesselHDG                      REAL            NOT NULL=False PK=False DEFAULT=None
  VesselSOG                      REAL            NOT NULL=False PK=False DEFAULT=None
  VesselCOG                      REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_Easting                  REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_Northing                 REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_Elevation                REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_Easting                  REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_Northing                 REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_Elevation                REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_INS_Easting               REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_INS_Northing              REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_USBL_Easting              REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_USBL_Northing             REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_Depth                     REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_HDG                       REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_SOG                       REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_COG                       REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_TMS_Easting               REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_TMS_Northing              REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_TMS_Depth                 REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_INS_Easting               REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_INS_Northing              REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_USBL_Easting              REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_USBL_Northing             REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_Depth                     REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_HDG                       REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_SOG                       REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_COG                       REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_TMS_Easting               REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_TMS_Northing              REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_TMS_Depth                 REAL            NOT NULL=False PK=False DEFAULT=None
  Crane_Easting                  REAL            NOT NULL=False PK=False DEFAULT=None
  Crane_Northing                 REAL            NOT NULL=False PK=False DEFAULT=None
  Crane_Depth                    REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_RefStation               TEXT            NOT NULL=False PK=False DEFAULT=None
  GNSS1_NOS                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS1_DiffAge                  REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_FixQuality               INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS1_HDOP                     REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_PDOP                     REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_VDOP                     REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_RefStation               TEXT            NOT NULL=False PK=False DEFAULT=None
  GNSS2_NOS                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS2_DiffAge                  REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_FixQuality               INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS2_HDOP                     REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_PDOP                     REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_VDOP                     REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_PITCH                     REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_ROLL                      REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_PITCH                     REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_ROLL                      REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_Depth1                    REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_Depth2                    REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_Depth1                    REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_Depth2                    REAL            NOT NULL=False PK=False DEFAULT=None
  Barometer                      REAL            NOT NULL=False PK=False DEFAULT=None
  File_FK                        INTEGER         NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  File_FK -> BlackBox_Files.ID ON UPDATE NO ACTION ON DELETE CASCADE

================================================================================
TABLE: BlackBox_FileStats

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  File_FK                        INTEGER         NOT NULL=True PK=False DEFAULT=None
  StartTime                      TEXT            NOT NULL=False PK=False DEFAULT=None
  EndTime                        TEXT            NOT NULL=False PK=False DEFAULT=None
  RowCount                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  DurationSec                    INTEGER         NOT NULL=False PK=False DEFAULT=None
  MaxTimeGapSec                  REAL            NOT NULL=False PK=False DEFAULT=None
  Config_FK                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  VesselEasting_Min              REAL            NOT NULL=False PK=False DEFAULT=None
  VesselEasting_Max              REAL            NOT NULL=False PK=False DEFAULT=None
  VesselNorthing_Min             REAL            NOT NULL=False PK=False DEFAULT=None
  VesselNorthing_Max             REAL            NOT NULL=False PK=False DEFAULT=None
  VesselElevation_Min            REAL            NOT NULL=False PK=False DEFAULT=None
  VesselElevation_Max            REAL            NOT NULL=False PK=False DEFAULT=None
  VesselHDG_Min                  REAL            NOT NULL=False PK=False DEFAULT=None
  VesselHDG_Max                  REAL            NOT NULL=False PK=False DEFAULT=None
  VesselSOG_Min                  REAL            NOT NULL=False PK=False DEFAULT=None
  VesselSOG_Max                  REAL            NOT NULL=False PK=False DEFAULT=None
  VesselCOG_Min                  REAL            NOT NULL=False PK=False DEFAULT=None
  VesselCOG_Max                  REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_SOG_Min                   REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_SOG_Max                   REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_SOG_Min                   REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_SOG_Max                   REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_Depth_Min                 REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_Depth_Max                 REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_Depth_Min                 REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_Depth_Max                 REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_Depth1_Min                REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_Depth1_Max                REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_Depth2_Min                REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_Depth2_Max                REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_Depth1_Min                REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_Depth1_Max                REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_Depth2_Min                REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_Depth2_Max                REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_HDOP_Min                 REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_HDOP_Max                 REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_PDOP_Min                 REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_PDOP_Max                 REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_VDOP_Min                 REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_VDOP_Max                 REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_HDOP_Min                 REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_HDOP_Max                 REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_PDOP_Min                 REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_PDOP_Max                 REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_VDOP_Min                 REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_VDOP_Max                 REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_NOS_Min                  INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS1_NOS_Max                  INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS1_DiffAge_Min              REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_DiffAge_Max              REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_FixQuality_Min           INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS1_FixQuality_Max           INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS2_NOS_Min                  INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS2_NOS_Max                  INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS2_DiffAge_Min              REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_DiffAge_Max              REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_FixQuality_Min           INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS2_FixQuality_Max           INTEGER         NOT NULL=False PK=False DEFAULT=None
  Barometer_Min                  REAL            NOT NULL=False PK=False DEFAULT=None
  Barometer_Max                  REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_PosDiff_Min               REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_PosDiff_Max               REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_PosDiff_Avg               REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_PosDiff_Min               REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_PosDiff_Max               REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_PosDiff_Avg               REAL            NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  File_FK -> BlackBox_Files.ID ON UPDATE NO ACTION ON DELETE CASCADE

================================================================================
TABLE: BlackBox_Files

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  FileName                       TEXT            NOT NULL=True PK=False DEFAULT=None
  Config_FK                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  UploadedAt                     TEXT            NOT NULL=False PK=False DEFAULT=datetime('now')

FOREIGN KEYS:
  Config_FK -> BBox_Configs_List.ID ON UPDATE NO ACTION ON DELETE CASCADE

================================================================================
TABLE: CSVLayers

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  Name                           TEXT            NOT NULL=False PK=False DEFAULT=None
  Points                         INTEGER         NOT NULL=False PK=False DEFAULT=None
  Attr1Name                      TEXT            NOT NULL=False PK=False DEFAULT=None
  Attr2Name                      TEXT            NOT NULL=False PK=False DEFAULT=None
  Attr3Name                      TEXT            NOT NULL=False PK=False DEFAULT=None
  PointStyle                     TEXT            NOT NULL=False PK=False DEFAULT='circle'
  PointColor                     TEXT            NOT NULL=False PK=False DEFAULT='#000000'
  PointSize                      INTEGER         NOT NULL=False PK=False DEFAULT=1
  Comments                       TEXT            NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: CSVpoints

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  Layer_FK                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  Point                          TEXT            NOT NULL=False PK=False DEFAULT=None
  X                              REAL            NOT NULL=False PK=False DEFAULT=None
  Y                              REAL            NOT NULL=False PK=False DEFAULT=None
  Z                              REAL            NOT NULL=False PK=False DEFAULT=None
  Attr1                          TEXT            NOT NULL=False PK=False DEFAULT=''
  Attr2                          INTEGER         NOT NULL=False PK=False DEFAULT=0
  Attr3                          REAL            NOT NULL=False PK=False DEFAULT=0

FOREIGN KEYS:
  Layer_FK -> CSVLayers.ID ON UPDATE NO ACTION ON DELETE CASCADE

================================================================================
TABLE: DSR

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  Solution_FK                    INTEGER         NOT NULL=True PK=False DEFAULT=1
  RLPreplot_FK                   INTEGER         NOT NULL=False PK=False DEFAULT=None
  LinePointIdx                   INTEGER         NOT NULL=False PK=False DEFAULT=None
  Line                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  Station                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  Node                           TEXT            NOT NULL=False PK=False DEFAULT=None
  NODE_HEX_ID                    INT             NOT NULL=True PK=False DEFAULT=None
  PreplotEasting                 REAL            NOT NULL=False PK=False DEFAULT=None
  PreplotNorthing                REAL            NOT NULL=False PK=False DEFAULT=None
  ROV                            TEXT            NOT NULL=False PK=False DEFAULT=None
  TimeStamp                      TEXT            NOT NULL=False PK=False DEFAULT=None
  PrimaryEasting                 REAL            NOT NULL=False PK=False DEFAULT=None
  Sigma                          REAL            NOT NULL=False PK=False DEFAULT=None
  PrimaryNorthing                REAL            NOT NULL=False PK=False DEFAULT=None
  Sigma1                         REAL            NOT NULL=False PK=False DEFAULT=None
  SecondaryEasting               REAL            NOT NULL=False PK=False DEFAULT=None
  Sigma2                         REAL            NOT NULL=False PK=False DEFAULT=None
  SecondaryNorthing              REAL            NOT NULL=False PK=False DEFAULT=None
  Sigma3                         REAL            NOT NULL=False PK=False DEFAULT=None
  DeltaEprimarytosecondary       REAL            NOT NULL=False PK=False DEFAULT=None
  DeltaNprimarytosecondary       REAL            NOT NULL=False PK=False DEFAULT=None
  Rangeprimarytosecondary        REAL            NOT NULL=False PK=False DEFAULT=None
  RangetoPrePlot                 REAL            NOT NULL=False PK=False DEFAULT=None
  BrgtoPrePlot                   REAL            NOT NULL=False PK=False DEFAULT=None
  PrimaryElevation               REAL            NOT NULL=False PK=False DEFAULT=None
  Sigma4                         REAL            NOT NULL=False PK=False DEFAULT=None
  SecondaryElevation             REAL            NOT NULL=False PK=False DEFAULT=None
  Sigma5                         REAL            NOT NULL=False PK=False DEFAULT=None
  Quality                        TEXT            NOT NULL=False PK=False DEFAULT=None
  ROV1                           TEXT            NOT NULL=False PK=False DEFAULT=None
  TimeStamp1                     TEXT            NOT NULL=False PK=False DEFAULT=None
  PrimaryEasting1                REAL            NOT NULL=False PK=False DEFAULT=None
  Sigma6                         REAL            NOT NULL=False PK=False DEFAULT=None
  PrimaryNorthing1               REAL            NOT NULL=False PK=False DEFAULT=None
  Sigma7                         REAL            NOT NULL=False PK=False DEFAULT=None
  SecondaryEasting1              REAL            NOT NULL=False PK=False DEFAULT=None
  Sigma8                         REAL            NOT NULL=False PK=False DEFAULT=None
  SecondaryNorthing1             REAL            NOT NULL=False PK=False DEFAULT=None
  Sigma9                         REAL            NOT NULL=False PK=False DEFAULT=None
  DeltaEprimarytosecondary1      REAL            NOT NULL=False PK=False DEFAULT=None
  DeltaNprimarytosecondary1      REAL            NOT NULL=False PK=False DEFAULT=None
  Rangeprimarytosecondary1       REAL            NOT NULL=False PK=False DEFAULT=None
  RangetoPrePlot1                REAL            NOT NULL=False PK=False DEFAULT=None
  BrgtoPrePlot1                  REAL            NOT NULL=False PK=False DEFAULT=None
  PrimaryElevation1              REAL            NOT NULL=False PK=False DEFAULT=None
  Sigma10                        REAL            NOT NULL=False PK=False DEFAULT=None
  SecondaryElevation1            REAL            NOT NULL=False PK=False DEFAULT=None
  Sigma11                        REAL            NOT NULL=False PK=False DEFAULT=None
  Quality1                       TEXT            NOT NULL=False PK=False DEFAULT=None
  DeployedtoRetrievedEasting     REAL            NOT NULL=False PK=False DEFAULT=None
  DeployedtoRetrievedNorthing    REAL            NOT NULL=False PK=False DEFAULT=None
  DeployedtoRecoveredElevation   REAL            NOT NULL=False PK=False DEFAULT=None
  DeployedtoRetrievedRange       REAL            NOT NULL=False PK=False DEFAULT=None
  DeployedtoRetrievedBrg         REAL            NOT NULL=False PK=False DEFAULT=None
  Comments                       TEXT            NOT NULL=False PK=False DEFAULT=None
  LinePoint                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  RecIdx                         INTEGER         NOT NULL=False PK=False DEFAULT=1
  Date                           TIMESTAMP       NOT NULL=False PK=False DEFAULT=CURRENT_TIMESTAMP
  Year                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  Month                          INTEGER         NOT NULL=False PK=False DEFAULT=None
  Week                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  Day                            TEXT            NOT NULL=False PK=False DEFAULT=None
  JDay                           INTEGER         NOT NULL=False PK=False DEFAULT=0
  Date1                          TIMESTAMP       NOT NULL=False PK=False DEFAULT=CURRENT_TIMESTAMP
  Year1                          INTEGER         NOT NULL=False PK=False DEFAULT=None
  Month1                         INTEGER         NOT NULL=False PK=False DEFAULT=None
  Week1                          INTEGER         NOT NULL=False PK=False DEFAULT=None
  Day1                           TEXT            NOT NULL=False PK=False DEFAULT=None
  JDay1                          INTEGER         NOT NULL=False PK=False DEFAULT=0
  DepTime                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  RecTime                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  PointComment                   TEXT            NOT NULL=False PK=False DEFAULT=None
  TIER                           INTEGER         NOT NULL=False PK=False DEFAULT=1
  isExported                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  isRecExported                  INTEGER         NOT NULL=False PK=False DEFAULT=0
  Area                           TEXT            NOT NULL=False PK=False DEFAULT=None
  RemoteUnit                     TEXT            NOT NULL=False PK=False DEFAULT=None
  AUQRCode                       TEXT            NOT NULL=False PK=False DEFAULT=None
  AURFID                         TEXT            NOT NULL=False PK=False DEFAULT=None
  CUSerialNumber                 TEXT            NOT NULL=False PK=False DEFAULT=None
  Status                         TEXT            NOT NULL=False PK=False DEFAULT=None
  DeploymentType                 TEXT            NOT NULL=False PK=False DEFAULT=None
  StartTimeEpoch                 INTEGER         NOT NULL=False PK=False DEFAULT=None
  StartTimeUTC                   TEXT            NOT NULL=False PK=False DEFAULT=None
  DeployTimeEpoch                INTEGER         NOT NULL=False PK=False DEFAULT=None
  DeployTimeUTC                  TEXT            NOT NULL=False PK=False DEFAULT=None
  PickupTimeEpoch                INTEGER         NOT NULL=False PK=False DEFAULT=None
  PickupTimeUTC                  TEXT            NOT NULL=False PK=False DEFAULT=None
  StopTimeEpoch                  INTEGER         NOT NULL=False PK=False DEFAULT=None
  StopTimeUTC                    TEXT            NOT NULL=False PK=False DEFAULT=None
  SPSX                           REAL            NOT NULL=False PK=False DEFAULT=None
  SPSY                           REAL            NOT NULL=False PK=False DEFAULT=None
  SPSZ                           REAL            NOT NULL=False PK=False DEFAULT=None
  ActualX                        REAL            NOT NULL=False PK=False DEFAULT=None
  ActualY                        REAL            NOT NULL=False PK=False DEFAULT=None
  ActualZ                        REAL            NOT NULL=False PK=False DEFAULT=None
  Deployed                       TEXT            NOT NULL=False PK=False DEFAULT=None
  PickedUp                       TEXT            NOT NULL=False PK=False DEFAULT=None
  Archived                       TEXT            NOT NULL=False PK=False DEFAULT=None
  DeviceID                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  BinID                          INTEGER         NOT NULL=False PK=False DEFAULT=None
  ExpectedTraces                 INTEGER         NOT NULL=False PK=False DEFAULT=None
  CollectedTraces                INTEGER         NOT NULL=False PK=False DEFAULT=None
  DownloadedDatainMB             INTEGER         NOT NULL=False PK=False DEFAULT=None
  ExpectedDatainMB               INTEGER         NOT NULL=False PK=False DEFAULT=None
  DownloadError                  INTEGER         NOT NULL=False PK=False DEFAULT=None
  DaysInWater                    INTEGER         NOT NULL=False PK=False DEFAULT=0
  TodayDaysInWater               INTEGER         NOT NULL=False PK=False DEFAULT=0

FOREIGN KEYS:
  RLPreplot_FK -> RLPreplot.ID ON UPDATE CASCADE ON DELETE SET NULL
  Solution_FK -> DSRSolution.ID ON UPDATE CASCADE ON DELETE RESTRICT

================================================================================
TABLE: DSRSolution

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  NAME                           TEXT            NOT NULL=True PK=False DEFAULT="Normal"

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: DSR_LineSummary

FIELDS:
  Line                           INT             NOT NULL=False PK=False DEFAULT=None
  PlannedPoints                                  NOT NULL=False PK=False DEFAULT=None
  isLineClicked                                  NOT NULL=False PK=False DEFAULT=None
  isLineDeployed                                 NOT NULL=False PK=False DEFAULT=None
  isValidated                                    NOT NULL=False PK=False DEFAULT=None
  DSRRows                                        NOT NULL=False PK=False DEFAULT=None
  Stations                                       NOT NULL=False PK=False DEFAULT=None
  Nodes                                          NOT NULL=False PK=False DEFAULT=None
  MinStation                                     NOT NULL=False PK=False DEFAULT=None
  MaxStation                                     NOT NULL=False PK=False DEFAULT=None
  DeployedCount                                  NOT NULL=False PK=False DEFAULT=None
  RetrievedCount                                 NOT NULL=False PK=False DEFAULT=None
  DepROVs                                        NOT NULL=False PK=False DEFAULT=None
  RecROVs                                        NOT NULL=False PK=False DEFAULT=None
  DepROVStats                                    NOT NULL=False PK=False DEFAULT=None
  RecROVStats                                    NOT NULL=False PK=False DEFAULT=None
  SMCount                                        NOT NULL=False PK=False DEFAULT=None
  SMRCount                                       NOT NULL=False PK=False DEFAULT=None
  ProcessedCount                                 NOT NULL=False PK=False DEFAULT=None
  FirstDeployTime                                NOT NULL=False PK=False DEFAULT=None
  LastDeployTime                                 NOT NULL=False PK=False DEFAULT=None
  DeploymentHours                                NOT NULL=False PK=False DEFAULT=None
  StartOfRec                                     NOT NULL=False PK=False DEFAULT=None
  EndOfRec                                       NOT NULL=False PK=False DEFAULT=None
  RecDuration                                    NOT NULL=False PK=False DEFAULT=None
  FirstRetrieveTime                              NOT NULL=False PK=False DEFAULT=None
  LastRetrieveTime                               NOT NULL=False PK=False DEFAULT=None
  RetrievalHours                                 NOT NULL=False PK=False DEFAULT=None
  TotalOperationHours                            NOT NULL=False PK=False DEFAULT=None
  DeployedPct                                    NOT NULL=False PK=False DEFAULT=None
  RetrievedPct                                   NOT NULL=False PK=False DEFAULT=None
  ProcessedPct                                   NOT NULL=False PK=False DEFAULT=None
  Normal                                         NOT NULL=False PK=False DEFAULT=None
  CoDeployed                                     NOT NULL=False PK=False DEFAULT=None
  Losted                                         NOT NULL=False PK=False DEFAULT=None
  Missplaced                                     NOT NULL=False PK=False DEFAULT=None
  WrongID                                        NOT NULL=False PK=False DEFAULT=None
  Overlap                                        NOT NULL=False PK=False DEFAULT=None
  AvgDeltaE                                      NOT NULL=False PK=False DEFAULT=None
  MinDeltaE                                      NOT NULL=False PK=False DEFAULT=None
  MaxDeltaE                                      NOT NULL=False PK=False DEFAULT=None
  AvgDeltaN                                      NOT NULL=False PK=False DEFAULT=None
  MinDeltaN                                      NOT NULL=False PK=False DEFAULT=None
  MaxDeltaN                                      NOT NULL=False PK=False DEFAULT=None
  AvgDeltaE1                                     NOT NULL=False PK=False DEFAULT=None
  MinDeltaE1                                     NOT NULL=False PK=False DEFAULT=None
  MaxDeltaE1                                     NOT NULL=False PK=False DEFAULT=None
  AvgDeltaN1                                     NOT NULL=False PK=False DEFAULT=None
  MinDeltaN1                                     NOT NULL=False PK=False DEFAULT=None
  MaxDeltaN1                                     NOT NULL=False PK=False DEFAULT=None
  AvgSigma                                       NOT NULL=False PK=False DEFAULT=None
  MinSigma                                       NOT NULL=False PK=False DEFAULT=None
  MaxSigma                                       NOT NULL=False PK=False DEFAULT=None
  AvgSigma1                                      NOT NULL=False PK=False DEFAULT=None
  MinSigma1                                      NOT NULL=False PK=False DEFAULT=None
  MaxSigma1                                      NOT NULL=False PK=False DEFAULT=None
  AvgSigma2                                      NOT NULL=False PK=False DEFAULT=None
  MinSigma2                                      NOT NULL=False PK=False DEFAULT=None
  MaxSigma2                                      NOT NULL=False PK=False DEFAULT=None
  AvgSigma3                                      NOT NULL=False PK=False DEFAULT=None
  MinSigma3                                      NOT NULL=False PK=False DEFAULT=None
  MaxSigma3                                      NOT NULL=False PK=False DEFAULT=None
  Primary_e95                                    NOT NULL=False PK=False DEFAULT=None
  Primary_n95                                    NOT NULL=False PK=False DEFAULT=None
  AvgRadOffset                                   NOT NULL=False PK=False DEFAULT=None
  MinRadOffset                                   NOT NULL=False PK=False DEFAULT=None
  MaxRadOffset                                   NOT NULL=False PK=False DEFAULT=None
  AvgRangePrimToSec                              NOT NULL=False PK=False DEFAULT=None
  MinRangePrimToSec                              NOT NULL=False PK=False DEFAULT=None
  MaxRangePrimToSec                              NOT NULL=False PK=False DEFAULT=None
  ID                             INT             NOT NULL=False PK=False DEFAULT=None
  Name                           TEXT            NOT NULL=False PK=False DEFAULT=None
  IsDefault                      INT             NOT NULL=False PK=False DEFAULT=None
  rov1_name                      TEXT            NOT NULL=False PK=False DEFAULT=None
  rov2_name                      TEXT            NOT NULL=False PK=False DEFAULT=None
  gnss1_name                     TEXT            NOT NULL=False PK=False DEFAULT=None
  gnss2_name                     TEXT            NOT NULL=False PK=False DEFAULT=None
  Vessel_name                    TEXT            NOT NULL=False PK=False DEFAULT=None
  Depth1_name                    TEXT            NOT NULL=False PK=False DEFAULT=None
  Depth2_name                    TEXT            NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: Files

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  FileName                       TEXT            NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: REC_DB

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  File_FK                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  Preplot_FK                     INTEGER         NOT NULL=False PK=False DEFAULT=None
  REC_ID                         INTEGER         NOT NULL=False PK=False DEFAULT=None
  NODE_ID                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  DEPLOY                         INTEGER         NOT NULL=False PK=False DEFAULT=None
  RPI                            INTEGER         NOT NULL=False PK=False DEFAULT=None
  PART_NO                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  Line                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  Point                          INTEGER         NOT NULL=False PK=False DEFAULT=None
  LinePoint                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  LinePointIdx                   INTEGER         NOT NULL=False PK=False DEFAULT=None
  TierLine                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  TierLinePoint                  INTEGER         NOT NULL=False PK=False DEFAULT=None
  TierLinePointIdx               INTEGER         NOT NULL=False PK=False DEFAULT=None
  RPRE_X                         REAL            NOT NULL=False PK=False DEFAULT=None
  RPRE_Y                         REAL            NOT NULL=False PK=False DEFAULT=None
  RFIELD_X                       REAL            NOT NULL=False PK=False DEFAULT=None
  RFIELD_Y                       REAL            NOT NULL=False PK=False DEFAULT=None
  RFIELD_Z                       REAL            NOT NULL=False PK=False DEFAULT=None
  REC_X                          REAL            NOT NULL=False PK=False DEFAULT=None
  REC_Y                          REAL            NOT NULL=False PK=False DEFAULT=None
  REC_Z                          REAL            NOT NULL=False PK=False DEFAULT=None
  TIMECORR                       REAL            NOT NULL=False PK=False DEFAULT=None
  BULKSHFT                       REAL            NOT NULL=False PK=False DEFAULT=None
  QDRIFT                         REAL            NOT NULL=False PK=False DEFAULT=None
  LDRIFT                         REAL            NOT NULL=False PK=False DEFAULT=None
  TRIMPTCH                       REAL            NOT NULL=False PK=False DEFAULT=None
  TRIMROLL                       REAL            NOT NULL=False PK=False DEFAULT=None
  TRIMYAW                        REAL            NOT NULL=False PK=False DEFAULT=None
  PITCHFIN                       REAL            NOT NULL=False PK=False DEFAULT=None
  ROLLFIN                        REAL            NOT NULL=False PK=False DEFAULT=None
  YAWFIN                         REAL            NOT NULL=False PK=False DEFAULT=None
  TOTDAYS                        REAL            NOT NULL=False PK=False DEFAULT=None
  RECCOUNT                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  CLKFLAG                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  EC1_RUS0                       REAL            NOT NULL=False PK=False DEFAULT=0
  EC1_RUS1                       REAL            NOT NULL=False PK=False DEFAULT=0
  EC1_EDT0                       REAL            NOT NULL=False PK=False DEFAULT=0
  EC1_EDT1                       REAL            NOT NULL=False PK=False DEFAULT=0
  EC1_EPT0                       REAL            NOT NULL=False PK=False DEFAULT=0
  EC1_EPT1                       REAL            NOT NULL=False PK=False DEFAULT=0
  NODSTART                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  DEPLOYTM                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  PICKUPTM                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  RUNTIME                        INTEGER         NOT NULL=False PK=False DEFAULT=0
  EC2_CD1                        INTEGER         NOT NULL=False PK=False DEFAULT=0
  TOTSHOTS                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  TOTPROD                        INTEGER         NOT NULL=False PK=False DEFAULT=0
  SPSK                           INTEGER         NOT NULL=False PK=False DEFAULT=0
  TIER                           INTEGER         NOT NULL=False PK=False DEFAULT=1

FOREIGN KEYS:
  File_FK -> Files.ID ON UPDATE CASCADE ON DELETE CASCADE
  Preplot_FK -> RLPreplot.ID ON UPDATE CASCADE ON DELETE SET NULL

================================================================================
TABLE: RLPreplot

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  Line                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  TierLine                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  Points                         INTEGER         NOT NULL=False PK=False DEFAULT=None
  UPoints                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  FirstPoint                     INTEGER         NOT NULL=False PK=False DEFAULT=None
  LastPoint                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  MinPoint                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  MaxPoint                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  RealStartX                     REAL            NOT NULL=False PK=False DEFAULT=None
  RealStartY                     REAL            NOT NULL=False PK=False DEFAULT=None
  RealEndX                       REAL            NOT NULL=False PK=False DEFAULT=None
  RealEndY                       REAL            NOT NULL=False PK=False DEFAULT=None
  StartX                         REAL            NOT NULL=False PK=False DEFAULT=None
  StartY                         REAL            NOT NULL=False PK=False DEFAULT=None
  EndX                           REAL            NOT NULL=False PK=False DEFAULT=None
  EndY                           REAL            NOT NULL=False PK=False DEFAULT=None
  LineLength                     REAL            NOT NULL=False PK=False DEFAULT=0
  RealLineLength                 REAL            NOT NULL=False PK=False DEFAULT=0
  LineBearing                    REAL            NOT NULL=False PK=False DEFAULT=0
  CalcLineBearing                REAL            NOT NULL=False PK=False DEFAULT=0
  isLineClicked                  INTEGER         NOT NULL=False PK=False DEFAULT=0
  isLineDeployed                 INTEGER         NOT NULL=False PK=False DEFAULT=0
  isLinePinged                   INTEGER         NOT NULL=False PK=False DEFAULT=0
  isLineRecovered                INTEGER         NOT NULL=False PK=False DEFAULT=0
  isMessaged                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  isValidated                    INTEGER         NOT NULL=False PK=False DEFAULT=0
  RPIndex                        INTEGER         NOT NULL=False PK=False DEFAULT=0
  Tier                           INTEGER         NOT NULL=False PK=False DEFAULT=1
  PointsDep                      INTEGER         NOT NULL=False PK=False DEFAULT=0
  PointsRec                      INTEGER         NOT NULL=False PK=False DEFAULT=0
  PointsProc                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  Comments                       TEXT            NOT NULL=False PK=False DEFAULT=''
  Message                        TEXT            NOT NULL=False PK=False DEFAULT=''
  ValidationTime                 TEXT            NOT NULL=False PK=False DEFAULT=None
  Spare1                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare2                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare3                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare4                         REAL            NOT NULL=False PK=False DEFAULT=0
  Spare5                         REAL            NOT NULL=False PK=False DEFAULT=0
  Spare6                         REAL            NOT NULL=False PK=False DEFAULT=0
  File_FK                        INTEGER         NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  File_FK -> Files.ID ON UPDATE NO ACTION ON DELETE CASCADE

================================================================================
TABLE: RLSolution

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  PPLine_FK                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  File_FK                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  LineName                       TEXT            NOT NULL=False PK=False DEFAULT=None
  Line                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  LineSolution                   INTEGER         NOT NULL=True PK=False DEFAULT=None
  Seq                            INTEGER         NOT NULL=False PK=False DEFAULT=1
  Attempt                        TEXT            NOT NULL=False PK=False DEFAULT=None
  Tier                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  TierLine                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  FRP                            INTEGER         NOT NULL=False PK=False DEFAULT=None
  LRP                            INTEGER         NOT NULL=False PK=False DEFAULT=None
  StartX                         REAL            NOT NULL=False PK=False DEFAULT=0
  StartY                         REAL            NOT NULL=False PK=False DEFAULT=0
  EndX                           REAL            NOT NULL=False PK=False DEFAULT=0
  EndY                           REAL            NOT NULL=False PK=False DEFAULT=0
  SRP                            INTEGER         NOT NULL=False PK=False DEFAULT=None
  ERP                            INTEGER         NOT NULL=False PK=False DEFAULT=None
  Vessel                         TEXT            NOT NULL=False PK=False DEFAULT=None
  StartYear                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  StartMonth                     INTEGER         NOT NULL=False PK=False DEFAULT=None
  StartJDay                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  StartDay                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  StartHour                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  StartMinute                    INTEGER         NOT NULL=False PK=False DEFAULT=None
  StartSecond                    INTEGER         NOT NULL=False PK=False DEFAULT=None
  StartMSecond                   REAL            NOT NULL=False PK=False DEFAULT=None
  EndYear                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  EndMonth                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  EndJDay                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  EndDay                         INTEGER         NOT NULL=False PK=False DEFAULT=None
  EndHour                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  EndMinute                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  EndSecond                      REAL            NOT NULL=False PK=False DEFAULT=None
  EndMSecond                     REAL            NOT NULL=False PK=False DEFAULT=None
  Solution_FK                    INTEGER         NOT NULL=False PK=False DEFAULT=None
  PercentOfLineDone              REAL            NOT NULL=False PK=False DEFAULT=None
  SeqProdCount                   REAL            NOT NULL=False PK=False DEFAULT=None
  PercentOFSeqDone               REAL            NOT NULL=False PK=False DEFAULT=None
  Count_All                      INTEGER         NOT NULL=False PK=False DEFAULT=0
  is_clicked                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  is_recovered                   INTEGER         NOT NULL=False PK=False DEFAULT=0
  is_fbloaded                    INTEGER         NOT NULL=False PK=False DEFAULT=0
  FileName_FK                    INTEGER         NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  FileName_FK -> Files.ID ON UPDATE NO ACTION ON DELETE CASCADE
  Solution_FK -> Solutions.ID ON UPDATE NO ACTION ON DELETE CASCADE
  PPLine_FK -> RLPreplot.ID ON UPDATE NO ACTION ON DELETE CASCADE

================================================================================
TABLE: ROVS_CONFIG

FIELDS:
  rov1_name                      TEXT            NOT NULL=False PK=False DEFAULT='ROV1'
  rov2_name                      TEXT            NOT NULL=False PK=False DEFAULT='ROV2'
  gnss1_name                     TEXT            NOT NULL=False PK=False DEFAULT='GNSS1'
  gnss2_name                     TEXT            NOT NULL=False PK=False DEFAULT='GNSS2'

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: RPPreplot

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  Line_FK                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  Line                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  TierLine                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  Point                          INTEGER         NOT NULL=False PK=False DEFAULT=None
  PointCode                      TEXT            NOT NULL=False PK=False DEFAULT=""
  PointIndex                     INTEGER         NOT NULL=False PK=False DEFAULT=1
  LinePoint                      INTEGER         NOT NULL=False PK=False DEFAULT=0
  TLinePoint                     REAL            NOT NULL=False PK=False DEFAULT=None
  LinePointIndex                 REAL            NOT NULL=False PK=False DEFAULT=0
  TLinePointIndex                REAL            NOT NULL=False PK=False DEFAULT=0
  X                              REAL            NOT NULL=False PK=False DEFAULT=None
  Y                              REAL            NOT NULL=False PK=False DEFAULT=None
  Z                              REAL            NOT NULL=False PK=False DEFAULT=None
  LineBearing                    REAL            NOT NULL=False PK=False DEFAULT=0
  Tier                           INTEGER         NOT NULL=False PK=False DEFAULT=1
  Spare1                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare2                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare3                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  File_FK                        INTEGER         NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  File_FK -> Files.ID ON UPDATE NO ACTION ON DELETE CASCADE
  Line_FK -> RLPreplot.ID ON UPDATE NO ACTION ON DELETE CASCADE

================================================================================
TABLE: RPSolution

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  LineName_FK                    INTEGER         NOT NULL=False PK=False DEFAULT=None
  Line                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  PP_Point_FK                    INTEGER         NOT NULL=False PK=False DEFAULT=None
  PP_Line_FK                     INTEGER         NOT NULL=False PK=False DEFAULT=None
  File_FK                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  Solution_FK                    INTEGER         NOT NULL=False PK=False DEFAULT=None
  Tier                           INTEGER         NOT NULL=False PK=False DEFAULT=0
  TierLinePoint                  INTEGER         NOT NULL=False PK=False DEFAULT=0
  LinePoint                      INTEGER         NOT NULL=False PK=False DEFAULT=0
  LinePointIdx                   INTEGER         NOT NULL=False PK=False DEFAULT=0
  LinePointIdxSol                INTEGER         NOT NULL=True PK=False DEFAULT=None
  Point                          INTEGER         NOT NULL=False PK=False DEFAULT=0
  PointIdx                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  FireCode                       TEXT            NOT NULL=False PK=False DEFAULT=None
  Seq                            INTEGER         NOT NULL=False PK=False DEFAULT=1
  ArrayNumber                    INTEGER         NOT NULL=False PK=False DEFAULT=None
  FCodeIdx                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  PointCode                      TEXT            NOT NULL=False PK=False DEFAULT=None
  Static                         REAL            NOT NULL=False PK=False DEFAULT=0
  PointDepth                     REAL            NOT NULL=False PK=False DEFAULT=0
  Datum                          INTEGER         NOT NULL=False PK=False DEFAULT=0
  Uphole                         REAL            NOT NULL=False PK=False DEFAULT=0
  WaterDepth                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  Easting                        REAL            NOT NULL=False PK=False DEFAULT=0
  Northing                       REAL            NOT NULL=False PK=False DEFAULT=0
  Elevation                      REAL            NOT NULL=False PK=False DEFAULT=0
  JDay                           INTEGER         NOT NULL=False PK=False DEFAULT=0
  Hour                           INTEGER         NOT NULL=False PK=False DEFAULT=0
  Minute                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Second                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Msecond                        REAL            NOT NULL=False PK=False DEFAULT=0
  Month                          INTEGER         NOT NULL=False PK=False DEFAULT=0
  Week                           INTEGER         NOT NULL=False PK=False DEFAULT=0
  Day                            INTEGER         NOT NULL=False PK=False DEFAULT=0
  Year                           INTEGER         NOT NULL=False PK=False DEFAULT=0
  TimeStamp                      TEXT            NOT NULL=False PK=False DEFAULT=None
  Date                           DATETIME        NOT NULL=False PK=False DEFAULT=None
  YearDay                        TEXT            NOT NULL=False PK=False DEFAULT=None
  Vessel                         TEXT            NOT NULL=False PK=False DEFAULT=None
  RadialOffset                   REAL            NOT NULL=False PK=False DEFAULT=0
  ILOffset                       REAL            NOT NULL=False PK=False DEFAULT=0
  XLOffset                       REAL            NOT NULL=False PK=False DEFAULT=0
  isCompared                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  isInSpec                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  isILInSpec                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  isXLInSpec                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  PP_X                           REAL            NOT NULL=False PK=False DEFAULT=0
  PP_Y                           REAL            NOT NULL=False PK=False DEFAULT=0
  dX                             REAL            NOT NULL=False PK=False DEFAULT=0
  dY                             REAL            NOT NULL=False PK=False DEFAULT=0
  isPreplotCompared              INTEGER         NOT NULL=False PK=False DEFAULT=0
  NODE_ID                        TEXT            NOT NULL=False PK=False DEFAULT=None
  DEPLOY                         INTEGER         NOT NULL=False PK=False DEFAULT=1
  RPI                            INTEGER         NOT NULL=False PK=False DEFAULT=1
  REC_X                          REAL            NOT NULL=False PK=False DEFAULT=0
  REC_Y                          REAL            NOT NULL=False PK=False DEFAULT=0
  REC_Z                          REAL            NOT NULL=False PK=False DEFAULT=0
  NEARILIN                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  NEARXLIN                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  TIMECORR                       REAL            NOT NULL=False PK=False DEFAULT=0
  BULKSHIFT                      REAL            NOT NULL=False PK=False DEFAULT=0
  TIMINGEQ                       REAL            NOT NULL=False PK=False DEFAULT=0
  QDRIFT                         REAL            NOT NULL=False PK=False DEFAULT=0
  LDRIFT                         REAL            NOT NULL=False PK=False DEFAULT=0
  TRIMPTCH                       REAL            NOT NULL=False PK=False DEFAULT=0
  TRIMROLL                       REAL            NOT NULL=False PK=False DEFAULT=0
  TRIMYAW                        REAL            NOT NULL=False PK=False DEFAULT=0
  PITCHFIN                       REAL            NOT NULL=False PK=False DEFAULT=0
  ROLLFIN                        REAL            NOT NULL=False PK=False DEFAULT=0
  YAWFIN                         REAL            NOT NULL=False PK=False DEFAULT=0
  TOTDAYS                        REAL            NOT NULL=False PK=False DEFAULT=0
  NODSTART                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  DEPLOYTM                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  PICKUPTM                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  RUNTIME                        INTEGER         NOT NULL=False PK=False DEFAULT=0
  EC2_CD1                        INTEGER         NOT NULL=False PK=False DEFAULT=0
  CLKFLAG                        INTEGER         NOT NULL=False PK=False DEFAULT=0
  EC1_RUS0                       REAL            NOT NULL=False PK=False DEFAULT=0
  EC1_RUS1                       REAL            NOT NULL=False PK=False DEFAULT=0
  EC1_EDT0                       REAL            NOT NULL=False PK=False DEFAULT=0
  EC1_EDT1                       REAL            NOT NULL=False PK=False DEFAULT=0
  EC1_EPT0                       REAL            NOT NULL=False PK=False DEFAULT=0
  EC1_EPT1                       REAL            NOT NULL=False PK=False DEFAULT=0
  TOTSHOTS                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  TOTPROD                        INTEGER         NOT NULL=False PK=False DEFAULT=0
  SPSK                           INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare1                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare2                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare3                         INTEGER         NOT NULL=False PK=False DEFAULT=0

FOREIGN KEYS:
  File_FK -> Files.ID ON UPDATE NO ACTION ON DELETE CASCADE
  PP_Line_FK -> RLPreplot.ID ON UPDATE NO ACTION ON DELETE CASCADE
  Solution_FK -> Solutions.ID ON UPDATE NO ACTION ON DELETE CASCADE
  PP_Point_FK -> RPPreplot.ID ON UPDATE NO ACTION ON DELETE CASCADE
  LineName_FK -> RLSolution.ID ON UPDATE NO ACTION ON DELETE CASCADE

================================================================================
TABLE: SHOT_LineSummary

FIELDS:
  nav_line_code                  TEXT            NOT NULL=False PK=False DEFAULT=None
  nav_line                                       NOT NULL=False PK=False DEFAULT=None
  attempt                                        NOT NULL=False PK=False DEFAULT=None
  seq                                            NOT NULL=False PK=False DEFAULT=None
  purpose_id                     INT             NOT NULL=False PK=False DEFAULT=None
  purpose                        TEXT            NOT NULL=False PK=False DEFAULT=None
  vessel_id                      INT             NOT NULL=False PK=False DEFAULT=None
  vessel_name                    TEXT            NOT NULL=False PK=False DEFAULT=None
  IsInSLSolution                                 NOT NULL=False PK=False DEFAULT=None
  ShotCount                                      NOT NULL=False PK=False DEFAULT=None
  ProdShots                                      NOT NULL=False PK=False DEFAULT=None
  NonProdShots                                   NOT NULL=False PK=False DEFAULT=None
  KillShots                                      NOT NULL=False PK=False DEFAULT=None
  FSP                                            NOT NULL=False PK=False DEFAULT=None
  LSP                                            NOT NULL=False PK=False DEFAULT=None
  FGSP                                           NOT NULL=False PK=False DEFAULT=None
  LGSP                                           NOT NULL=False PK=False DEFAULT=None
  Sum_shot_station                               NOT NULL=False PK=False DEFAULT=None
  Sum_shot_index                                 NOT NULL=False PK=False DEFAULT=None
  Sum_shot_status                                NOT NULL=False PK=False DEFAULT=None
  Sum_seq                                        NOT NULL=False PK=False DEFAULT=None
  Sum_post_point_code                            NOT NULL=False PK=False DEFAULT=None
  Sum_fire_code                                  NOT NULL=False PK=False DEFAULT=None
  Sum_gun_depth                                  NOT NULL=False PK=False DEFAULT=None
  Sum_water_depth                                NOT NULL=False PK=False DEFAULT=None
  Sum_shot_x                                     NOT NULL=False PK=False DEFAULT=None
  Sum_shot_y                                     NOT NULL=False PK=False DEFAULT=None
  Sum_shot_day                                   NOT NULL=False PK=False DEFAULT=None
  Sum_shot_hour                                  NOT NULL=False PK=False DEFAULT=None
  Sum_shot_minute                                NOT NULL=False PK=False DEFAULT=None
  Sum_shot_second                                NOT NULL=False PK=False DEFAULT=None
  Sum_shot_microsecond                           NOT NULL=False PK=False DEFAULT=None
  Sum_shot_year                                  NOT NULL=False PK=False DEFAULT=None
  Sum_array_id                                   NOT NULL=False PK=False DEFAULT=None
  Sum_source_id                                  NOT NULL=False PK=False DEFAULT=None
  Sum_nav_station                                NOT NULL=False PK=False DEFAULT=None
  Sum_shot_group_id                              NOT NULL=False PK=False DEFAULT=None
  Sum_elevation                                  NOT NULL=False PK=False DEFAULT=None
  sps_SailLine                   TEXT            NOT NULL=False PK=False DEFAULT=None
  sps_Line                       INT             NOT NULL=False PK=False DEFAULT=None
  sps_Attempt                    TEXT            NOT NULL=False PK=False DEFAULT=None
  sps_Seq                        INT             NOT NULL=False PK=False DEFAULT=None
  sps_Sum_Line                                   NOT NULL=False PK=False DEFAULT=None
  sps_Sum_Seq                                    NOT NULL=False PK=False DEFAULT=None
  sps_Sum_Point                                  NOT NULL=False PK=False DEFAULT=None
  sps_Sum_PointCode                              NOT NULL=False PK=False DEFAULT=None
  sps_Sum_FireCode                               NOT NULL=False PK=False DEFAULT=None
  sps_Sum_ArrayCode                              NOT NULL=False PK=False DEFAULT=None
  sps_Sum_Static                                 NOT NULL=False PK=False DEFAULT=None
  sps_Sum_PointDepth                             NOT NULL=False PK=False DEFAULT=None
  sps_Sum_Datum                                  NOT NULL=False PK=False DEFAULT=None
  sps_Sum_Uphole                                 NOT NULL=False PK=False DEFAULT=None
  sps_Sum_WaterDepth                             NOT NULL=False PK=False DEFAULT=None
  sps_Sum_Easting                                NOT NULL=False PK=False DEFAULT=None
  sps_Sum_Northing                               NOT NULL=False PK=False DEFAULT=None
  sps_Sum_Elevation                              NOT NULL=False PK=False DEFAULT=None
  sps_Sum_JDay                                   NOT NULL=False PK=False DEFAULT=None
  sps_Sum_Hour                                   NOT NULL=False PK=False DEFAULT=None
  sps_Sum_Minute                                 NOT NULL=False PK=False DEFAULT=None
  sps_Sum_Second                                 NOT NULL=False PK=False DEFAULT=None
  sps_Sum_Microsecond                            NOT NULL=False PK=False DEFAULT=None
  cmp_Line                                       NOT NULL=False PK=False DEFAULT=None
  cmp_Attempt                                    NOT NULL=False PK=False DEFAULT=None
  cmp_Seq                                        NOT NULL=False PK=False DEFAULT=None
  cmp_Point                                      NOT NULL=False PK=False DEFAULT=None
  cmp_PointCode                                  NOT NULL=False PK=False DEFAULT=None
  cmp_FireCode                                   NOT NULL=False PK=False DEFAULT=None
  cmp_WaterDepth                                 NOT NULL=False PK=False DEFAULT=None
  cmp_Easting                                    NOT NULL=False PK=False DEFAULT=None
  cmp_Northing                                   NOT NULL=False PK=False DEFAULT=None
  cmp_Elevation                                  NOT NULL=False PK=False DEFAULT=None
  cmp_JDay                                       NOT NULL=False PK=False DEFAULT=None
  cmp_Hour                                       NOT NULL=False PK=False DEFAULT=None
  cmp_Minute                                     NOT NULL=False PK=False DEFAULT=None
  cmp_Second                                     NOT NULL=False PK=False DEFAULT=None
  cmp_Microsecond                                NOT NULL=False PK=False DEFAULT=None
  diff_Attempt                                   NOT NULL=False PK=False DEFAULT=None
  diff_Seq                                       NOT NULL=False PK=False DEFAULT=None
  diff_Point                                     NOT NULL=False PK=False DEFAULT=None
  diff_PointCode                                 NOT NULL=False PK=False DEFAULT=None
  diff_FireCode                                  NOT NULL=False PK=False DEFAULT=None
  diff_WaterDepth                                NOT NULL=False PK=False DEFAULT=None
  diff_Easting                                   NOT NULL=False PK=False DEFAULT=None
  diff_Northing                                  NOT NULL=False PK=False DEFAULT=None
  diff_Elevation                                 NOT NULL=False PK=False DEFAULT=None
  diff_JDay                                      NOT NULL=False PK=False DEFAULT=None
  diff_Hour                                      NOT NULL=False PK=False DEFAULT=None
  diff_Minute                                    NOT NULL=False PK=False DEFAULT=None
  diff_Second                                    NOT NULL=False PK=False DEFAULT=None
  diff_Microsecond                               NOT NULL=False PK=False DEFAULT=None
  SumDiff                                        NOT NULL=False PK=False DEFAULT=None
  QC_AllMatch                                    NOT NULL=False PK=False DEFAULT=None
  QC_AnyMatch                                    NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: SHOT_TABLE

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  sail_line                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  shot_station                   INTEGER         NOT NULL=False PK=False DEFAULT=None
  shot_index                     INTEGER         NOT NULL=False PK=False DEFAULT=None
  shot_status                    INTEGER         NOT NULL=False PK=False DEFAULT=None
  nav_line_code                  TEXT            NOT NULL=False PK=False DEFAULT=None
  nav_line                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  attempt                        TEXT            NOT NULL=False PK=False DEFAULT=None
  seq                            INTEGER         NOT NULL=False PK=False DEFAULT=None
  post_point_code                TEXT            NOT NULL=False PK=False DEFAULT=None
  fire_code                      TEXT            NOT NULL=False PK=False DEFAULT=None
  gun_depth                      REAL            NOT NULL=False PK=False DEFAULT=None
  water_depth                    REAL            NOT NULL=False PK=False DEFAULT=None
  shot_x                         REAL            NOT NULL=False PK=False DEFAULT=None
  shot_y                         REAL            NOT NULL=False PK=False DEFAULT=None
  shot_day                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  shot_hour                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  shot_minute                    INTEGER         NOT NULL=False PK=False DEFAULT=None
  shot_second                    INTEGER         NOT NULL=False PK=False DEFAULT=None
  shot_microsecond               INTEGER         NOT NULL=False PK=False DEFAULT=None
  shot_year                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  vessel                         TEXT            NOT NULL=False PK=False DEFAULT=None
  array_id                       TEXT            NOT NULL=False PK=False DEFAULT=None
  source_id                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  nav_station                    INTEGER         NOT NULL=False PK=False DEFAULT=None
  shot_group_id                  INTEGER         NOT NULL=False PK=False DEFAULT=None
  elevation                      REAL            NOT NULL=False PK=False DEFAULT=None
  File_FK                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  Seq_FK                         INTEGER         NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  Seq_FK -> sequence_vessel_assignment.ID ON UPDATE NO ACTION ON DELETE SET NULL
  File_FK -> STFiles.id ON UPDATE NO ACTION ON DELETE SET NULL

================================================================================
TABLE: SLPreplot

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  Line                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  TierLine                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  Points                         INTEGER         NOT NULL=False PK=False DEFAULT=None
  UPoints                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  FirstPoint                     INTEGER         NOT NULL=False PK=False DEFAULT=None
  LastPoint                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  MinPoint                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  MaxPoint                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  RealStartX                     REAL            NOT NULL=False PK=False DEFAULT=None
  RealStartY                     REAL            NOT NULL=False PK=False DEFAULT=None
  RealEndX                       REAL            NOT NULL=False PK=False DEFAULT=None
  RealEndY                       REAL            NOT NULL=False PK=False DEFAULT=None
  StartX                         REAL            NOT NULL=False PK=False DEFAULT=None
  StartY                         REAL            NOT NULL=False PK=False DEFAULT=None
  EndX                           REAL            NOT NULL=False PK=False DEFAULT=None
  EndY                           REAL            NOT NULL=False PK=False DEFAULT=None
  LineLength                     REAL            NOT NULL=False PK=False DEFAULT=0
  RealLineLength                 REAL            NOT NULL=False PK=False DEFAULT=0
  LineBearing                    REAL            NOT NULL=False PK=False DEFAULT=0
  CalcLineBearing                REAL            NOT NULL=False PK=False DEFAULT=0
  isLineClicked                  INTEGER         NOT NULL=False PK=False DEFAULT=0
  isLineDeployed                 INTEGER         NOT NULL=False PK=False DEFAULT=0
  isLinePinged                   INTEGER         NOT NULL=False PK=False DEFAULT=0
  isLineRecovered                INTEGER         NOT NULL=False PK=False DEFAULT=0
  isMessaged                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  isValidated                    INTEGER         NOT NULL=False PK=False DEFAULT=0
  RPIndex                        INTEGER         NOT NULL=False PK=False DEFAULT=0
  Tier                           INTEGER         NOT NULL=False PK=False DEFAULT=1
  PointsDep                      INTEGER         NOT NULL=False PK=False DEFAULT=0
  PointsRec                      INTEGER         NOT NULL=False PK=False DEFAULT=0
  PointsProc                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  Comments                       TEXT            NOT NULL=False PK=False DEFAULT=''
  Message                        TEXT            NOT NULL=False PK=False DEFAULT=''
  ValidationTime                 TEXT            NOT NULL=False PK=False DEFAULT=None
  Spare1                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare2                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare3                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare4                         REAL            NOT NULL=False PK=False DEFAULT=0
  Spare5                         REAL            NOT NULL=False PK=False DEFAULT=0
  Spare6                         REAL            NOT NULL=False PK=False DEFAULT=0
  File_FK                        INTEGER         NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  File_FK -> Files.ID ON UPDATE NO ACTION ON DELETE CASCADE

================================================================================
TABLE: SLSolution

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  PPLine_FK                      INTEGER         NOT NULL=False PK=False DEFAULT=0
  File_FK                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  SailLine                       TEXT            NOT NULL=False PK=False DEFAULT=None
  Line                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  Seq                            INTEGER         NOT NULL=False PK=False DEFAULT=None
  Attempt                        TEXT            NOT NULL=False PK=False DEFAULT=None
  Tier                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  TierLine                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  FSP                            INTEGER         NOT NULL=False PK=False DEFAULT=None
  LSP                            INTEGER         NOT NULL=False PK=False DEFAULT=None
  FGSP                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  LGSP                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  StartX                         DOUBLE          NOT NULL=False PK=False DEFAULT=None
  StartY                         DOUBLE          NOT NULL=False PK=False DEFAULT=None
  EndX                           DOUBLE          NOT NULL=False PK=False DEFAULT=None
  EndY                           DOUBLE          NOT NULL=False PK=False DEFAULT=None
  Vessel_FK                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  Start_Time                     DATETIME        NOT NULL=False PK=False DEFAULT=None
  End_Time                       DATETIME        NOT NULL=False PK=False DEFAULT=None
  LineLength                     REAL            NOT NULL=False PK=False DEFAULT=0
  Start_Production_Time          DATETIME        NOT NULL=False PK=False DEFAULT=None
  End_Production_Time            DATETIME        NOT NULL=False PK=False DEFAULT=None
  PercentOfLineCompleted         REAL            NOT NULL=False PK=False DEFAULT=None
  PercentOfSeqCompleted          REAL            NOT NULL=False PK=False DEFAULT=None
  ProductionCount                INTEGER         NOT NULL=False PK=False DEFAULT=None
  NonProductionCount             INTEGER         NOT NULL=False PK=False DEFAULT=None
  KillCount                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  MinGunDepth                    REAL            NOT NULL=False PK=False DEFAULT=None
  MaxGunDepth                    REAL            NOT NULL=False PK=False DEFAULT=None
  MinProdGunDepth                REAL            NOT NULL=False PK=False DEFAULT=0
  MaxProdGunDepth                REAL            NOT NULL=False PK=False DEFAULT=0
  MinNonProdGunDepth             REAL            NOT NULL=False PK=False DEFAULT=0
  MaxNonProdGunDepth             REAL            NOT NULL=False PK=False DEFAULT=0
  MinWaterDepth                  REAL            NOT NULL=False PK=False DEFAULT=None
  MaxWaterDepth                  REAL            NOT NULL=False PK=False DEFAULT=None
  MinProdWaterDepth              REAL            NOT NULL=False PK=False DEFAULT=None
  MaxProdWaterDepth              REAL            NOT NULL=False PK=False DEFAULT=None
  MinNonProdWaterDepth           REAL            NOT NULL=False PK=False DEFAULT=None
  MaxNonProdWaterDepth           REAL            NOT NULL=False PK=False DEFAULT=None
  MinKillGunDepth                REAL            NOT NULL=False PK=False DEFAULT=None
  MaxKillGunDepth                REAL            NOT NULL=False PK=False DEFAULT=None
  MinKillWaterDepth              REAL            NOT NULL=False PK=False DEFAULT=None
  MaxKillWaterDepth              REAL            NOT NULL=False PK=False DEFAULT=None
  PP_Length                      REAL            NOT NULL=False PK=False DEFAULT=None
  SeqLenPercentage               REAL            NOT NULL=False PK=False DEFAULT=None
  MaxSPI                         REAL            NOT NULL=False PK=False DEFAULT=None
  MaxSeq                         INTEGER         NOT NULL=False PK=False DEFAULT=None
  purpose_id                     INTEGER         NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  Vessel_FK -> project_fleet.ID ON UPDATE CASCADE ON DELETE CASCADE
  PPLine_FK -> SLPreplot.ID ON UPDATE CASCADE ON DELETE NO ACTION
  File_FK -> Files.ID ON UPDATE CASCADE ON DELETE CASCADE

================================================================================
TABLE: SPPreplot

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  Line_FK                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  Line                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  TierLine                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  Point                          INTEGER         NOT NULL=False PK=False DEFAULT=None
  PointCode                      TEXT            NOT NULL=False PK=False DEFAULT=""
  PointIndex                     INTEGER         NOT NULL=False PK=False DEFAULT=1
  LinePoint                      INTEGER         NOT NULL=False PK=False DEFAULT=0
  TLinePoint                     REAL            NOT NULL=False PK=False DEFAULT=None
  LinePointIndex                 REAL            NOT NULL=False PK=False DEFAULT=0
  TLinePointIndex                REAL            NOT NULL=False PK=False DEFAULT=0
  X                              REAL            NOT NULL=False PK=False DEFAULT=None
  Y                              REAL            NOT NULL=False PK=False DEFAULT=None
  Z                              REAL            NOT NULL=False PK=False DEFAULT=None
  LineBearing                    REAL            NOT NULL=False PK=False DEFAULT=0
  Tier                           INTEGER         NOT NULL=False PK=False DEFAULT=1
  Spare1                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare2                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare3                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  File_FK                        INTEGER         NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  File_FK -> Files.ID ON UPDATE NO ACTION ON DELETE CASCADE
  Line_FK -> SLPreplot.ID ON UPDATE NO ACTION ON DELETE CASCADE

================================================================================
TABLE: SPS_BathyGrid

FIELDS:
  cell_size                      REAL            NOT NULL=True PK=True DEFAULT=None
  fire_code                      TEXT            NOT NULL=True PK=True DEFAULT=''
  gx                             REAL            NOT NULL=True PK=True DEFAULT=None
  gy                             REAL            NOT NULL=True PK=True DEFAULT=None
  depth_avg                      REAL            NOT NULL=False PK=False DEFAULT=None
  depth_min                      REAL            NOT NULL=False PK=False DEFAULT=None
  depth_max                      REAL            NOT NULL=False PK=False DEFAULT=None
  point_count                    INTEGER         NOT NULL=True PK=False DEFAULT=0

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: SPS_Files

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  FileName                       TEXT            NOT NULL=True PK=False DEFAULT=None
  FileSize                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  FileType                       TEXT            NOT NULL=False PK=False DEFAULT='NOAR_R_SPS'
  CreatedAt                      TEXT            NOT NULL=False PK=False DEFAULT=CURRENT_TIMESTAMP

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: SPSolution

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  SailLine_FK                    INTEGER         NOT NULL=False PK=False DEFAULT=None
  PPLine_FK                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  Vessel_FK                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  File_FK                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  SailLine                       TEXT            NOT NULL=False PK=False DEFAULT=None
  Line                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  Attempt                        TEXT            NOT NULL=False PK=False DEFAULT=None
  Seq                            INTEGER         NOT NULL=False PK=False DEFAULT=None
  Tier                           INTEGER         NOT NULL=False PK=False DEFAULT=0
  TierLinePoint                  INTEGER         NOT NULL=False PK=False DEFAULT=0
  LinePoint                      INTEGER         NOT NULL=False PK=False DEFAULT=0
  PointIdx                       INTEGER         NOT NULL=False PK=False DEFAULT=None
  Point                          INTEGER         NOT NULL=False PK=False DEFAULT=0
  PointCode                      TEXT            NOT NULL=False PK=False DEFAULT=None
  FireCode                       TEXT            NOT NULL=False PK=False DEFAULT=None
  ArrayCode                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  Static                         REAL            NOT NULL=False PK=False DEFAULT=0
  PointDepth                     REAL            NOT NULL=False PK=False DEFAULT=0
  Datum                          INTEGER         NOT NULL=False PK=False DEFAULT=0
  Uphole                         REAL            NOT NULL=False PK=False DEFAULT=0
  WaterDepth                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  Easting                        REAL            NOT NULL=False PK=False DEFAULT=0
  Northing                       REAL            NOT NULL=False PK=False DEFAULT=0
  Elevation                      REAL            NOT NULL=False PK=False DEFAULT=0
  JDay                           INTEGER         NOT NULL=False PK=False DEFAULT=0
  Hour                           INTEGER         NOT NULL=False PK=False DEFAULT=0
  Minute                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Second                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Microsecond                    REAL            NOT NULL=False PK=False DEFAULT=0
  Month                          INTEGER         NOT NULL=False PK=False DEFAULT=0
  Week                           INTEGER         NOT NULL=False PK=False DEFAULT=0
  Day                            INTEGER         NOT NULL=False PK=False DEFAULT=0
  Year                           INTEGER         NOT NULL=False PK=False DEFAULT=0
  YearDay                        TEXT            NOT NULL=False PK=False DEFAULT=None
  TimeStamp                      DATETIME        NOT NULL=False PK=False DEFAULT=None
  Vessel                         TEXT            NOT NULL=False PK=False DEFAULT=None
  RadialOffset                   REAL            NOT NULL=False PK=False DEFAULT=0
  ILOffset                       REAL            NOT NULL=False PK=False DEFAULT=0
  XLOffset                       REAL            NOT NULL=False PK=False DEFAULT=0
  isCompared                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  isInSpec                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  isILInSpec                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  isXLInSpec                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  PP_X                           REAL            NOT NULL=False PK=False DEFAULT=0
  PP_Y                           REAL            NOT NULL=False PK=False DEFAULT=0
  dX                             REAL            NOT NULL=False PK=False DEFAULT=0
  dY                             REAL            NOT NULL=False PK=False DEFAULT=0
  isPreplotCompared              INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare1                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare2                         INTEGER         NOT NULL=False PK=False DEFAULT=0
  Spare3                         INTEGER         NOT NULL=False PK=False DEFAULT=0

FOREIGN KEYS:
  Vessel_FK -> project_fleet.ID ON UPDATE CASCADE ON DELETE CASCADE
  File_FK -> Files.ID ON UPDATE CASCADE ON DELETE CASCADE
  PPLine_FK -> SLPreplot.ID ON UPDATE CASCADE ON DELETE NO ACTION
  SailLine_FK -> SLSolution.ID ON UPDATE CASCADE ON DELETE CASCADE

================================================================================
TABLE: STDeletedLines

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  nav_line_code                  TEXT            NOT NULL=True PK=False DEFAULT=None
  deleted_at                     TEXT            NOT NULL=False PK=False DEFAULT=CURRENT_TIMESTAMP
  deleted_by                     TEXT            NOT NULL=False PK=False DEFAULT=None
  restore_mode                   TEXT            NOT NULL=False PK=False DEFAULT='manual'

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: STFileLines

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  stfile_id                      INTEGER         NOT NULL=True PK=False DEFAULT=None
  nav_line_code                  TEXT            NOT NULL=True PK=False DEFAULT=None
  byte_start                     INTEGER         NOT NULL=True PK=False DEFAULT=None
  byte_end                       INTEGER         NOT NULL=True PK=False DEFAULT=None
  first_nav_station              INTEGER         NOT NULL=False PK=False DEFAULT=None
  last_nav_station               INTEGER         NOT NULL=False PK=False DEFAULT=None
  row_count                      INTEGER         NOT NULL=True PK=False DEFAULT=0
  checksum                       TEXT            NOT NULL=False PK=False DEFAULT=None
  created_at                     TEXT            NOT NULL=False PK=False DEFAULT=CURRENT_TIMESTAMP

FOREIGN KEYS:
  stfile_id -> STFiles.id ON UPDATE NO ACTION ON DELETE CASCADE

================================================================================
TABLE: STFiles

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  file_name                      TEXT            NOT NULL=True PK=False DEFAULT=None
  file_size                      INTEGER         NOT NULL=True PK=False DEFAULT=0
  file_mtime                     TEXT            NOT NULL=False PK=False DEFAULT=None
  file_hash                      TEXT            NOT NULL=False PK=False DEFAULT=None
  previous_stfile_id             INTEGER         NOT NULL=False PK=False DEFAULT=None
  previous_file_size             INTEGER         NOT NULL=True PK=False DEFAULT=0
  start_byte                     INTEGER         NOT NULL=True PK=False DEFAULT=0
  end_byte                       INTEGER         NOT NULL=True PK=False DEFAULT=0
  last_read_byte                 INTEGER         NOT NULL=True PK=False DEFAULT=0
  import_mode                    TEXT            NOT NULL=True PK=False DEFAULT='full'
  row_count                      INTEGER         NOT NULL=True PK=False DEFAULT=0
  inserted_count                 INTEGER         NOT NULL=True PK=False DEFAULT=0
  duplicate_count                INTEGER         NOT NULL=True PK=False DEFAULT=0
  changed_lines_count            INTEGER         NOT NULL=True PK=False DEFAULT=0
  deleted_lines_count            INTEGER         NOT NULL=True PK=False DEFAULT=0
  created_at                     TEXT            NOT NULL=False PK=False DEFAULT=CURRENT_TIMESTAMP
  updated_at                     TEXT            NOT NULL=False PK=False DEFAULT=CURRENT_TIMESTAMP

FOREIGN KEYS:
  previous_stfile_id -> STFiles.id ON UPDATE NO ACTION ON DELETE SET NULL

================================================================================
TABLE: Solutions

FIELDS:
  ID                             INTEGER         NOT NULL=True PK=True DEFAULT=None
  Solution                       TEXT            NOT NULL=True PK=False DEFAULT=None
  Comments                       TEXT            NOT NULL=False PK=False DEFAULT=''
  SpareInt1                      INTEGER         NOT NULL=False PK=False DEFAULT=NULL
  SpareInt2                      INTEGER         NOT NULL=False PK=False DEFAULT=NULL
  SpareText1                     TEXT            NOT NULL=False PK=False DEFAULT=''
  SpareText2                     TEXT            NOT NULL=False PK=False DEFAULT=''
  IsBase                         INTEGER         NOT NULL=True PK=False DEFAULT=0

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: ocr_results

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  image_path                     TEXT            NOT NULL=True PK=False DEFAULT=None
  image_name                     TEXT            NOT NULL=False PK=False DEFAULT=None
  resolution                     TEXT            NOT NULL=False PK=False DEFAULT=None
  config_used                    TEXT            NOT NULL=False PK=False DEFAULT=None
  file_role                      TEXT            NOT NULL=False PK=False DEFAULT=None
  file_line                      TEXT            NOT NULL=False PK=False DEFAULT=None
  file_station                   TEXT            NOT NULL=False PK=False DEFAULT=None
  file_index                     TEXT            NOT NULL=False PK=False DEFAULT=None
  rov                            TEXT            NOT NULL=False PK=False DEFAULT=None
  dive                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  date                           TEXT            NOT NULL=False PK=False DEFAULT=None
  time                           TEXT            NOT NULL=False PK=False DEFAULT=None
  line                           TEXT            NOT NULL=False PK=False DEFAULT=None
  station                        TEXT            NOT NULL=False PK=False DEFAULT=None
  east                           TEXT            NOT NULL=False PK=False DEFAULT=None
  north                          TEXT            NOT NULL=False PK=False DEFAULT=None
  dsr_line                       TEXT            NOT NULL=False PK=False DEFAULT=None
  dsr_station                    TEXT            NOT NULL=False PK=False DEFAULT=None
  dsr_x                          TEXT            NOT NULL=False PK=False DEFAULT=None
  dsr_y                          TEXT            NOT NULL=False PK=False DEFAULT=None
  dsr_timestamp                  TEXT            NOT NULL=False PK=False DEFAULT=None
  dsr_timestamp1                 TEXT            NOT NULL=False PK=False DEFAULT=None
  dsr_rov                        TEXT            NOT NULL=False PK=False DEFAULT=None
  dsr_rov1                       TEXT            NOT NULL=False PK=False DEFAULT=None
  delta_m                        REAL            NOT NULL=False PK=False DEFAULT=None
  ocr_vs_file                    TEXT            NOT NULL=False PK=False DEFAULT=None
  file_vs_dsr                    TEXT            NOT NULL=False PK=False DEFAULT=None
  status                         TEXT            NOT NULL=False PK=False DEFAULT=None
  station_image_count            INTEGER         NOT NULL=False PK=False DEFAULT=None
  expected_images                TEXT            NOT NULL=False PK=False DEFAULT=None
  station_status                 TEXT            NOT NULL=False PK=False DEFAULT=None
  message                        TEXT            NOT NULL=False PK=False DEFAULT=None
  checked                        INTEGER         NOT NULL=True PK=False DEFAULT=1
  processed_at                   TEXT            NOT NULL=False PK=False DEFAULT=CURRENT_TIMESTAMP

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: project_fleet

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  vessel_name                    TEXT            NOT NULL=True PK=False DEFAULT=None
  imo                            TEXT            NOT NULL=False PK=False DEFAULT=None
  mmsi                           TEXT            NOT NULL=False PK=False DEFAULT=None
  call_sign                      TEXT            NOT NULL=False PK=False DEFAULT=None
  vessel_type                    TEXT            NOT NULL=False PK=False DEFAULT=None
  owner                          TEXT            NOT NULL=False PK=False DEFAULT=None
  is_active                      INTEGER         NOT NULL=False PK=False DEFAULT=1
  is_retired                     INTEGER         NOT NULL=False PK=False DEFAULT=0
  notes                          TEXT            NOT NULL=False PK=False DEFAULT=None
  source_vessel_id               INTEGER         NOT NULL=False PK=False DEFAULT=None
  created_at                     TEXT            NOT NULL=False PK=False DEFAULT=None
  updated_at                     TEXT            NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: project_folders

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  shapes_folder                  TEXT            NOT NULL=False PK=False DEFAULT=None
  image_folder                   TEXT            NOT NULL=False PK=False DEFAULT=None
  local_prj_folder               TEXT            NOT NULL=False PK=False DEFAULT=None
  bb_folder                      TEXT            NOT NULL=False PK=False DEFAULT=None
  segy_folder                    TEXT            NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: project_geometry

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  rpi                            REAL            NOT NULL=True PK=False DEFAULT=None
  rli                            REAL            NOT NULL=True PK=False DEFAULT=None
  spi                            REAL            NOT NULL=True PK=False DEFAULT=None
  sli                            REAL            NOT NULL=True PK=False DEFAULT=None
  rl_heading                     REAL            NOT NULL=True PK=False DEFAULT=None
  sl_heading                     REAL            NOT NULL=True PK=False DEFAULT=None
  production_code                TEXT            NOT NULL=True PK=False DEFAULT=None
  non_production_code            TEXT            NOT NULL=True PK=False DEFAULT=None
  kill_code                      TEXT            NOT NULL=True PK=False DEFAULT=None
  rl_mask                        TEXT            NOT NULL=True PK=False DEFAULT=None
  sl_mask                        TEXT            NOT NULL=True PK=False DEFAULT=None
  sail_line_mask                 TEXT            NOT NULL=True PK=False DEFAULT=None

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: project_gun_qc

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  num_of_arrays                  INTEGER         NOT NULL=True PK=False DEFAULT=None
  num_of_strings                 INTEGER         NOT NULL=True PK=False DEFAULT=None
  num_of_guns                    INTEGER         NOT NULL=True PK=False DEFAULT=None
  depth                          REAL            NOT NULL=True PK=False DEFAULT=None
  depth_tolerance                REAL            NOT NULL=True PK=False DEFAULT=None
  time_warning                   REAL            NOT NULL=True PK=False DEFAULT=None
  time_error                     REAL            NOT NULL=True PK=False DEFAULT=None
  pressure                       REAL            NOT NULL=True PK=False DEFAULT=None
  pressure_drop                  REAL            NOT NULL=True PK=False DEFAULT=None
  volume                         REAL            NOT NULL=True PK=False DEFAULT=None
  max_il_offset                  REAL            NOT NULL=True PK=False DEFAULT=None
  max_xl_offset                  REAL            NOT NULL=True PK=False DEFAULT=None
  max_radial_offset              REAL            NOT NULL=True PK=False DEFAULT=None
  kill_shots_cons                INTEGER         NOT NULL=False PK=False DEFAULT=None
  percentage_of_kill             INTEGER         NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: project_main

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  name                           TEXT            NOT NULL=True PK=False DEFAULT=None
  location                       TEXT            NOT NULL=True PK=False DEFAULT=None
  area                           TEXT            NOT NULL=True PK=False DEFAULT=None
  client                         TEXT            NOT NULL=True PK=False DEFAULT=None
  contractor                     TEXT            NOT NULL=True PK=False DEFAULT=None
  project_client_id              TEXT            NOT NULL=True PK=False DEFAULT=None
  project_contractor_id          TEXT            NOT NULL=True PK=False DEFAULT=None
  epsg                           TEXT            NOT NULL=True PK=False DEFAULT=None
  line_code                      TEXT            NOT NULL=True PK=False DEFAULT=None
  start_project                  TEXT            NOT NULL=True PK=False DEFAULT=None
  project_duration               INTEGER         NOT NULL=True PK=False DEFAULT=None
  color_scheme                   TEXT            NOT NULL=False PK=False DEFAULT='dark'

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: project_node_qc

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  max_il_offset                  REAL            NOT NULL=True PK=False DEFAULT=None
  max_xl_offset                  REAL            NOT NULL=True PK=False DEFAULT=None
  max_radial_offset              REAL            NOT NULL=True PK=False DEFAULT=None
  percent_of_depth               REAL            NOT NULL=True PK=False DEFAULT=None
  use_offset                     INTEGER         NOT NULL=True PK=False DEFAULT=None
  battery_life                   INTEGER         NOT NULL=False PK=False DEFAULT=0
  gnss_diffage_warning           INTEGER         NOT NULL=False PK=False DEFAULT=0
  gnss_diffage_error             INTEGER         NOT NULL=False PK=False DEFAULT=0
  gnss_fixed_quality             INTEGER         NOT NULL=False PK=False DEFAULT=0
  max_sma                        REAL            NOT NULL=False PK=False DEFAULT=0
  warning_sma                    REAL            NOT NULL=False PK=False DEFAULT=0

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: project_shapes

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  FullName                       TEXT            NOT NULL=True PK=True DEFAULT=None
  FileName                       TEXT            NOT NULL=False PK=False DEFAULT=None
  isFilled                       INTEGER         NOT NULL=False PK=False DEFAULT=0
  FillColor                      TEXT            NOT NULL=False PK=False DEFAULT='#000000'
  LineColor                      TEXT            NOT NULL=False PK=False DEFAULT='#000000'
  LineWidth                      INTEGER         NOT NULL=False PK=False DEFAULT=1
  LineStyle                      TEXT            NOT NULL=False PK=False DEFAULT=''
  HatchPattern                   TEXT            NOT NULL=False PK=False DEFAULT=''
  FileCheck                      INT             NOT NULL=False PK=False DEFAULT=1

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: project_template

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  FirstSL                        INTEGER         NOT NULL=False PK=False DEFAULT=None
  LastSL                         INTEGER         NOT NULL=False PK=False DEFAULT=None
  LNum                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  RLine                          INTEGER         NOT NULL=False PK=False DEFAULT=None
  Tier                           INTEGER         NOT NULL=False PK=False DEFAULT=None
  deployed_by_vessel             INTEGER         NOT NULL=False PK=False DEFAULT=None
  recovered_by_vessel            INTEGER         NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  recovered_by_vessel -> project_fleet.ID ON UPDATE NO ACTION ON DELETE NO ACTION
  deployed_by_vessel -> project_fleet.ID ON UPDATE NO ACTION ON DELETE NO ACTION

================================================================================
TABLE: project_template_sl_groups

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  group_no                       INTEGER         NOT NULL=True PK=False DEFAULT=None
  start_line                     INTEGER         NOT NULL=True PK=False DEFAULT=None
  end_line                       INTEGER         NOT NULL=True PK=False DEFAULT=None
  direction                      TEXT            NOT NULL=True PK=False DEFAULT='asc'
  is_active                      INTEGER         NOT NULL=True PK=False DEFAULT=1

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: project_vessels

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  Name                           TEXT            NOT NULL=True PK=False DEFAULT=None
  Description                    TEXT            NOT NULL=False PK=False DEFAULT=None
  IMONum                         TEXT            NOT NULL=True PK=False DEFAULT=None

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: rov_box

FIELDS:
  ID                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  TimeStamp                      TEXT            NOT NULL=False PK=False DEFAULT=None
  VesselEasting                  REAL            NOT NULL=False PK=False DEFAULT=None
  VesselNorthing                 REAL            NOT NULL=False PK=False DEFAULT=None
  VesselElevation                REAL            NOT NULL=False PK=False DEFAULT=None
  VesselHDG                      REAL            NOT NULL=False PK=False DEFAULT=None
  VesselSOG                      REAL            NOT NULL=False PK=False DEFAULT=None
  VesselCOG                      REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_INS_Easting               REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_INS_Northing              REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_USBL_Easting              REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_USBL_Northing             REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1Depth                      REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1HDG                        REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1SOG                        REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1COG                        REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_TMS_Easting               REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_TMS_Northing              REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_TMS_Depth                 REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_INS_Easting               REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_INS_Northing              REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_USBL_Easting              REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_USBL_Northing             REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2Depth                      REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2HDG                        REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2SOG                        REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2COG                        REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_TMS_Easting               REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_TMS_Northing              REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_TMS_Depth                 REAL            NOT NULL=False PK=False DEFAULT=None
  Crane_Easting                  REAL            NOT NULL=False PK=False DEFAULT=None
  Crane_Northing                 REAL            NOT NULL=False PK=False DEFAULT=None
  Crane_Depth                    REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_NOS                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS1_DiffAge                  REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_FixQuality               INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS1_HDOP                     REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_PDOP                     REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS1_VDOP                     REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_NOS                      INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS2_DiffAge                  REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_FixQuality               INTEGER         NOT NULL=False PK=False DEFAULT=None
  GNSS2_HDOP                     REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_PDOP                     REAL            NOT NULL=False PK=False DEFAULT=None
  GNSS2_VDOP                     REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_PITCH                     REAL            NOT NULL=False PK=False DEFAULT=None
  ROV1_ROLL                      REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_PITCH                     REAL            NOT NULL=False PK=False DEFAULT=None
  ROV2_ROLL                      REAL            NOT NULL=False PK=False DEFAULT=None
  Vessel                         TEXT            NOT NULL=False PK=False DEFAULT=None
  ROV1                           TEXT            NOT NULL=False PK=False DEFAULT=None
  ROV2                           TEXT            NOT NULL=False PK=False DEFAULT=None
  GNSS1                          TEXT            NOT NULL=False PK=False DEFAULT=None
  GNSS2                          TEXT            NOT NULL=False PK=False DEFAULT=None
  FileName                       TEXT            NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: sequence_vessel_assignment

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  seq_first                      INTEGER         NOT NULL=True PK=False DEFAULT=None
  seq_last                       INTEGER         NOT NULL=True PK=False DEFAULT=None
  vessel_id                      INTEGER         NOT NULL=True PK=False DEFAULT=None
  purpose                        TEXT            NOT NULL=False PK=False DEFAULT=None
  purpose_id                     INTEGER         NOT NULL=True PK=False DEFAULT=4
  comments                       TEXT            NOT NULL=False PK=False DEFAULT=None
  is_active                      INTEGER         NOT NULL=False PK=False DEFAULT=1
  created_at                     TEXT            NOT NULL=False PK=False DEFAULT=CURRENT_TIMESTAMP
  updated_at                     TEXT            NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  vessel_id -> project_fleet.id ON UPDATE NO ACTION ON DELETE CASCADE

================================================================================
TABLE: svp_format_setups

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  name                           TEXT            NOT NULL=False PK=False DEFAULT=None
  file_ext                       TEXT            NOT NULL=False PK=False DEFAULT=None
  delimiter                      TEXT            NOT NULL=False PK=False DEFAULT=None
  header_line_count              INTEGER         NOT NULL=False PK=False DEFAULT=None
  data_header_line_index         INTEGER         NOT NULL=False PK=False DEFAULT=None
  data_start_line_index          INTEGER         NOT NULL=False PK=False DEFAULT=None
  meta_coordinates_key           TEXT            NOT NULL=False PK=False DEFAULT=None
  meta_lat_key                   TEXT            NOT NULL=False PK=False DEFAULT=None
  meta_lon_key                   TEXT            NOT NULL=False PK=False DEFAULT=None
  meta_rov_key                   TEXT            NOT NULL=False PK=False DEFAULT=None
  meta_timestamp_key             TEXT            NOT NULL=False PK=False DEFAULT=None
  meta_name_key                  TEXT            NOT NULL=False PK=False DEFAULT=None
  meta_location_key              TEXT            NOT NULL=False PK=False DEFAULT=None
  meta_serial_key                TEXT            NOT NULL=False PK=False DEFAULT=None
  meta_make_key                  TEXT            NOT NULL=False PK=False DEFAULT=None
  meta_model_key                 TEXT            NOT NULL=False PK=False DEFAULT=None
  col_timestamp                  TEXT            NOT NULL=False PK=False DEFAULT=None
  col_depth                      TEXT            NOT NULL=False PK=False DEFAULT=None
  col_velocity                   TEXT            NOT NULL=False PK=False DEFAULT=None
  col_temperature                TEXT            NOT NULL=False PK=False DEFAULT=None
  col_salinity                   TEXT            NOT NULL=False PK=False DEFAULT=None
  col_density                    TEXT            NOT NULL=False PK=False DEFAULT=None
  sort_by_depth                  INTEGER         NOT NULL=False PK=False DEFAULT=None
  clamp_negative_depth_to_zero   INTEGER         NOT NULL=False PK=False DEFAULT=None
  pressure_is_depth              INTEGER         NOT NULL=False PK=False DEFAULT=None
  notes                          TEXT            NOT NULL=False PK=False DEFAULT=None
  created_at                     TEXT            NOT NULL=False PK=False DEFAULT=None
  updated_at                     TEXT            NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  No foreign keys

================================================================================
TABLE: svp_points

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  svp_id                         INTEGER         NOT NULL=True PK=False DEFAULT=None
  point_index                    INTEGER         NOT NULL=True PK=False DEFAULT=None
  depth_m                        REAL            NOT NULL=True PK=False DEFAULT=None
  velocity_mps                   REAL            NOT NULL=True PK=False DEFAULT=None
  temperature_c                  REAL            NOT NULL=False PK=False DEFAULT=None
  salinity_psu                   REAL            NOT NULL=False PK=False DEFAULT=None
  density_kgm3                   REAL            NOT NULL=False PK=False DEFAULT=None
  source_row_text                TEXT            NOT NULL=False PK=False DEFAULT=None

FOREIGN KEYS:
  svp_id -> svp_profiles.id ON UPDATE NO ACTION ON DELETE CASCADE

================================================================================
TABLE: svp_profiles

FIELDS:
  id                             INTEGER         NOT NULL=False PK=True DEFAULT=None
  name                           TEXT            NOT NULL=False PK=False DEFAULT=None
  profile_source                 TEXT            NOT NULL=False PK=False DEFAULT=None
  file_type                      TEXT            NOT NULL=False PK=False DEFAULT=None
  location                       TEXT            NOT NULL=False PK=False DEFAULT=None
  instrument_make                TEXT            NOT NULL=False PK=False DEFAULT=None
  instrument_model               TEXT            NOT NULL=False PK=False DEFAULT=None
  serial                         TEXT            NOT NULL=False PK=False DEFAULT=None
  rov                            TEXT            NOT NULL=False PK=False DEFAULT=None
  timestamp                      TEXT            NOT NULL=False PK=False DEFAULT=None
  latitude                       REAL            NOT NULL=False PK=False DEFAULT=None
  longitude                      REAL            NOT NULL=False PK=False DEFAULT=None
  coord_e                        REAL            NOT NULL=False PK=False DEFAULT=None
  coord_n                        REAL            NOT NULL=False PK=False DEFAULT=None
  casts                          TEXT            NOT NULL=False PK=False DEFAULT=None
  surface_velocity               REAL            NOT NULL=False PK=False DEFAULT=None
  mean_velocity                  REAL            NOT NULL=False PK=False DEFAULT=None
  seabed_velocity                REAL            NOT NULL=False PK=False DEFAULT=None
  bottom_depth                   REAL            NOT NULL=False PK=False DEFAULT=None
  mean_density                   REAL            NOT NULL=False PK=False DEFAULT=None
  temperature_surface            REAL            NOT NULL=False PK=False DEFAULT=None
  salinity_surface               REAL            NOT NULL=False PK=False DEFAULT=None
  source_file_name               TEXT            NOT NULL=False PK=False DEFAULT=None
  source_file_path               TEXT            NOT NULL=False PK=False DEFAULT=None
  raw_header                     TEXT            NOT NULL=False PK=False DEFAULT=None
  notes                          TEXT            NOT NULL=False PK=False DEFAULT=None
  created_at                     TEXT            NOT NULL=True PK=False DEFAULT=None
  updated_at                     TEXT            NOT NULL=True PK=False DEFAULT=None

FOREIGN KEYS:
  No foreign keys

Process finished with exit code 0

Task: