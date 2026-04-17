import subprocess
import sys
from pathlib import Path

import pytest
from PyInstaller import __main__ as pyi_main


@pytest.mark.slow
def test_frozen_app_can_import_arrow_odbc(tmp_path: Path) -> None:
    app_name = "userapp"
    app = tmp_path / f"{app_name}.py"
    app.write_text("import arrow_odbc\nprint('loaded')\n")

    pyi_main.run([
        "--workpath", str(tmp_path / "build"),
        "--distpath", str(tmp_path / "dist"),
        "--specpath", str(tmp_path),
        "--noconfirm",
        "--log-level=WARN",
        str(app),
    ])

    suffix = ".exe" if sys.platform == "win32" else ""
    frozen = tmp_path / "dist" / app_name / f"{app_name}{suffix}"
    run = subprocess.run([str(frozen)], capture_output=True, text=True)
    assert run.returncode == 0, f"stdout={run.stdout!r}\nstderr={run.stderr!r}"
    assert "loaded" in run.stdout
