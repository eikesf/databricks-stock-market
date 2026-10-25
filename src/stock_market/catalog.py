"""Unity Catalog names for one environment, built from the catalog the job receives."""

import re
from dataclasses import dataclass

PRICES = "prices"
METADATA = "metadata"

_CATALOG_PATTERN = re.compile(r"[a-z][a-z0-9_]*")


@dataclass(frozen=True)
class CatalogNames:
    """Fully qualified table names and volume paths for one environment.

    The catalog is always passed in (job parameter or pipeline configuration). This
    class never reads it from an environment variable and has no default (DL-007).

    Attributes:
        catalog: Unity Catalog catalog of the target, the value of ``var.catalog``.
    """

    catalog: str

    def __post_init__(self) -> None:
        """Reject an empty or malformed catalog before any name is built."""
        if not _CATALOG_PATTERN.fullmatch(self.catalog):
            raise ValueError(f"Invalid catalog name: {self.catalog!r}")

    @property
    def prices_raw(self) -> str:
        """Bronze table with raw daily prices."""
        return f"{self.catalog}.bronze.prices_raw"

    @property
    def metadata_raw(self) -> str:
        """Bronze table with raw company metadata."""
        return f"{self.catalog}.bronze.metadata_raw"

    @property
    def landing_dir(self) -> str:
        """Root of the landing volume written by the producers and read by Auto Loader."""
        return f"/Volumes/{self.catalog}/bronze/landing"

    @property
    def landing_prices_dir(self) -> str:
        """Landing folder for daily price files."""
        return f"{self.landing_dir}/{PRICES}"

    @property
    def landing_metadata_dir(self) -> str:
        """Landing folder for company metadata files."""
        return f"{self.landing_dir}/{METADATA}"
