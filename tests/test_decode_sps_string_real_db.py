import shutil
from pathlib import Path

import pytest
from django.conf import settings

from core.projectdb import ProjectDB
from core.models import SPSRevision
TEST_DB_DIR = Path("D:/02_MyProgram/099_TEMP")  # any folder

@pytest.fixture(scope="session")
def fixed_db_dir():
    TEST_DB_DIR.mkdir(parents=True, exist_ok=True)
    return TEST_DB_DIR

@pytest.fixture
def use_real_sqlite_copy(tmp_path):
    # Реальный файл базы Django (не test/in-memory)
    #real_db = Path(settings.BASE_DIR) / "db.sqlite3"   # <-- если у тебя другое имя - поменяй здесь
    real_db = Path("D:/02_MyProgram/03_TEST_DATA/test2/data/project.sqlite3")
    if not real_db.exists():
        pytest.skip(f"Real sqlite DB not found: {real_db}")

    db_copy = tmp_path / "db_copy.sqlite3"
    shutil.copy2(real_db, db_copy)

    return ProjectDB(str(db_copy))
@pytest.fixture
def real_db_copy(fixed_db_dir):
    real_db = Path("D:/02_MyProgram/03_TEST_DATA/test2/data/project.sqlite3")

    db_copy = fixed_db_dir / "db_copy.sqlite3"
    shutil.copy2(real_db, db_copy)

    return ProjectDB(str(db_copy))

@pytest.mark.django_db
def test_decode_with_real_django_db_copy(real_db_copy):
    pdb = real_db_copy
    geom = pdb.get_geometry()

    rev = SPSRevision.objects.filter(id=2).first()
    assert rev is not None
    preplot_file = Path("D:/04_PROJECTS/03_CGG_LACONIA/Preplot/Phase 1/Production_Phase1_RX_CGG_Laconia_V2.0.r01")
    s = "R53271               28941                     544788.8 2970198.6   0.0         "

    #out = ProjectDB.decode_sps_string(s, rev, geom, default=None, tier=1, point_type="R")
    #assert out is not None
    if not preplot_file.exists():
        pytest.skip(f"Test SPS file not found: {preplot_file}")

    sps_points = pdb.load_sps_file(
        file_path=str(preplot_file),
        sps_revision=rev,
        default=None,
        tier=1,
        point_type="R",
        line_bearing=0
    )

    assert sps_points is not None
    assert len(sps_points) > 0
