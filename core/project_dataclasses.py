from dataclasses import dataclass, field
from datetime import date


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
    rl_mask: str = "LLLLPPPP"
    sl_mask: str = "LLLLXSSSS"

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
        if not self.sl_mask:
            return 0
        num_X = self.sl_mask.count("X")
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
