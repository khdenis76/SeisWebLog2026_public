from dataclasses import dataclass, field
from datetime import date,datetime,timedelta


# ======================= DATA CLASSES =======================
@dataclass
class MainSettings:
    """
    Main project-level metadata stored in project_main table.
    """
    name: str = "New project"
    location: str = "N/A"
    area: str = "Gulf of America"
    client: str = "No name"
    contractor: str = "No name"
    project_client_id: str = "PRJ000"
    project_contractor_id: str = "PRJ000"
    epsg: str = "26716"
    line_code: str = "AAAAA"
    # Start date of the project (default: today)
    start_project: str = field(default_factory=lambda: date.today().isoformat())
    # Estimated duration of the project in days
    project_duration: int = 30
    color_scheme:str ='dark'
@dataclass
class GeometrySettings:
    """
    Geometry parameters of the seismic project.
    """
    rpi: float = 0.0
    rli: float = 0.0
    spi: float = 0.0
    sli: float = 0.0
    rl_heading: float = 360.0
    sl_heading: float = 0.0
    production_code: str = "AP"
    non_production_code: str = "LRMXTK"
    kill_code:str="KX"
    rl_mask: str = "LLLLPPPP"
    sl_mask: str = "LLLLPPPP"
    sail_line_mask: str = "LLLLLXSSSS"

    @property
    def rec_point_length(self) -> int:
        """Receiver point number length (10^n)."""
        if not self.rl_mask:
            return 0
        num_P = self.rl_mask.count("P")
        return 10 ** (num_P + 1)
    @property
    def rec_line_length(self) -> int:
        """line number length (10^n)."""
        if not self.rl_mask:
            return 0
        num_L = self.rl_mask.count("L")
        return 10 ** (num_L + 1)

    @property
    def sou_line_length(self) -> int:
        """line number length (10^n)."""
        if not self.sl_mask:
            return 0
        num_L = self.rl_mask.count("L")
        return 10 ** (num_L + 1)

    @property
    def rec_linepoint_length(self) -> int:
        """Receiver line-point number length (10^n)."""
        if not self.rl_mask:
            return 0
        num_L = self.rl_mask.count("L")
        return 10 ** (num_L + 1)

    @property
    def sou_point_length(self) -> int:
        """Source point number length (10^n)."""
        if not self.sl_mask:
            return 0
        num_P = self.sl_mask.count("P")
        return 10 ** (num_P + 1)

    @property
    def sou_linepoint_length(self) -> int:
        """Source line-point number length (10^n)."""
        if not self.sl_mask:
            return 0
        num_L = self.sl_mask.count("L")
        return 10 ** (num_L + 1)

    @property
    def sou_attempt_length(self) -> int:
        """Source attempt number length (10^n)."""
        if not self.sail_line_mask:
            return 0
        num_X = self.sail_line_mask.count("X")
        return 10 ** num_X
@dataclass
class NodeQCSettings:
    """
    QC tolerances for node-based systems (OBN).
    """
    max_il_offset: float = 0.0
    max_xl_offset: float = 0.0
    max_radial_offset: float = 0.0
    percent_of_depth: float = 0.0
    max_sma:float = 0.0
    warning_sma:float = 0.0
    # 0 = radial, 1 = inline, 2 = crossline
    use_offset: int = 0
    battery_life: int = 0
    gnss_diffage_warning: int = 20
    gnss_diffage_error: int = 30
    gnss_fixed_quality: int =5
@dataclass
class GunQCSettings:
    """
    QC tolerances and configuration for source gun arrays.
    """
    num_of_arrays: int = 3
    num_of_strings: int = 3
    num_of_guns: int = 3
    depth: float = 0.0
    depth_tolerance: float = 5.0
    time_warning: float = 1.0
    time_error: float = 1.5
    pressure: float = 2000.0
    pressure_drop: float = 100.0
    volume: float = 4000.0
    max_il_offset: float = 0.0
    max_xl_offset: float = 0.0
    max_radial_offset: float = 0.0
    kill_shots_cons: int=0
    percentage_of_kill:int=0

@dataclass
class FolderSettings:
    """Additional folder for project"""
    shapes_folder:str="N/A"
    image_folder:str="N/A"
    local_prj_folder:str="N/A"
    bb_folder:str="N/A"
    segy_folder:str="N/A"
@dataclass
class PreplotData:
    """
     Class for SPS data import to preplot db
    """
    line_fk: int | None = None
    filе_fk: int | None = None
    line:int =0
    point:int =0
    point_code:str =""
    point_index:int =1
    easting:float=0.0
    northing:float=0.0
    elevation:float=0.0
    line_point:int=0
    tier: int =1
    tier_line:int=0
    line_point_idx:int=0
    tier_line_point:int=0
    tier_line_point_idx:int=0
    line_bearing:float=0

@dataclass(slots=True)
class SourceSPSData:
    # FK/meta
    sail_line_fk: int = 0
    ppline_fk: int = 0
    vessel_fk: int | None = None
    file_fk: int = 0

    # identifiers (from SailLine mask)
    sail_line: str = ""
    line: int = 0
    attempt: str = ""
    seq: int = 0
    tier: int = 1

    # point
    point_idx: int = 1
    point: int = 0
    point_code: str = ""
    fire_code: str = ""
    array_code: int = 0

    # depths/coords
    point_depth: float = 0.0
    water_depth: float = 0.0
    easting: float = 0.0
    northing: float = 0.0
    elevation: float = 0.0

    # derived indexing
    line_point: int = 0
    tier_line_point: int = 0

    # time parts
    jday: int = 1
    hour: int = 0
    minute: int = 0
    second: int = 0
    microsecond: int = 0
    year: int = field(default_factory=lambda: date.today().year)

    # cached datetime
    _cached_dt: datetime = field(init=False, repr=False)

    @staticmethod
    def _clean_int(x, default=0) -> int:
        if x is None:
            return default
        if isinstance(x, int):
            return x
        s = str(x).strip()
        num = ""
        for ch in s:
            if ch.isdigit():
                num += ch
            else:
                break
        return int(num) if num else default

    def __post_init__(self):
        y = self._clean_int(self.year, default=date.today().year)
        j = self._clean_int(self.jday, default=1)

        if j < 1:
            j = 1
        if j > 366:
            j = 366

        base = datetime.strptime(f"{y}-{j:03d}", "%Y-%j")

        self._cached_dt = base + timedelta(
            hours=self._clean_int(self.hour),
            minutes=self._clean_int(self.minute),
            seconds=self._clean_int(self.second),
            microseconds=self._clean_int(self.microsecond),
        )

    @property
    def timestamp(self) -> datetime:
        return self._cached_dt

    @property
    def month(self) -> int:
        return self._cached_dt.month

    @property
    def week(self) -> int:
        return self._cached_dt.isocalendar().week

    @property
    def day(self) -> int:
        return self._cached_dt.day

    @property
    def yearday(self) -> str:
        return f"{self.year}-{self.jday:03d}"

    def to_db_tuple(self) -> tuple:
        """
        Must match INSERT column order exactly (see insert_sql below)
        """
        return (
            self.sail_line_fk,
            self.ppline_fk,
            self.vessel_fk,
            self.file_fk,

            self.sail_line,
            self.line,
            self.attempt,
            self.seq,
            self.tier,

            self.tier_line_point,
            self.line_point,
            self.point_idx,
            self.point,

            self.point_code,
            self.fire_code,
            self.array_code,

            self.point_depth,
            self.water_depth,
            self.easting,
            self.northing,
            self.elevation,

            self.jday,
            self.hour,
            self.minute,
            self.second,
            self.microsecond,

            self.month,
            self.week,
            self.day,
            self.year,
            self.yearday,

            self.timestamp.isoformat(sep=" "),
        )

