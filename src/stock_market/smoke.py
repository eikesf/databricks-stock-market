import sys

from stock_market import __version__

def main() -> None:
    print(f"stock_market {__version__} on Python {sys.version.split()[0]}")