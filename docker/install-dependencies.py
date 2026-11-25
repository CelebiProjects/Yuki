"""Install REANA and the native Snakemake CLI in separate environments.

REANA's validation extras pin Snakemake 7, while native execution needs 8/9.
Keep REANA's Python imports in the service environment and expose the newer
native CLI through a symlink, without adding its packages to service sys.path.
"""
import pathlib
import subprocess
import sys
import tomllib


def main():
    root = pathlib.Path(__file__).resolve().parent.parent
    config = tomllib.loads((root / "pyproject.toml").read_text())
    dependencies = config["project"]["dependencies"]
    native = [item for item in dependencies if item.startswith("snakemake")]
    service = [item for item in dependencies if item not in native]
    if len(native) != 1:
        raise RuntimeError("Expected one native Snakemake requirement")
    subprocess.run([sys.executable, "-m", "pip", "install", *service], check=True)
    environment = pathlib.Path("/opt/yuki-native")
    subprocess.run([sys.executable, "-m", "venv", str(environment)], check=True)
    subprocess.run(
        [str(environment / "bin/python"), "-m", "pip", "install", *native],
        check=True,
    )
    executable = pathlib.Path("/usr/local/bin/snakemake")
    executable.unlink(missing_ok=True)
    executable.symlink_to(environment / "bin/snakemake")


if __name__ == "__main__":
    main()
