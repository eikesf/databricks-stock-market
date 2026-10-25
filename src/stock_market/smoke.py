import sys
from importlib.metadata import version

from stock_market import __version__


def main() -> None:
    """Print package, Python and key dependency versions to prove the wheel installed and runs."""
    print(  # noqa: T201 - smoke output is the job's stdout
        f"stock_market {__version__} on Python {sys.version.split()[0]} | "
        f"pandas {version('pandas')} | pyarrow {version('pyarrow')}"
    )
