import dataclasses
import re
from pathlib import Path

import pytest

from stock_market.catalog import CatalogNames


def test_names_are_built_from_the_given_catalog():
    names = CatalogNames("my_catalog")
    assert names.prices_raw == "my_catalog.bronze.prices_raw"
    assert names.metadata_raw == "my_catalog.bronze.metadata_raw"
    assert names.landing_prices_dir == "/Volumes/my_catalog/bronze/landing/prices"
    assert names.landing_metadata_dir == "/Volumes/my_catalog/bronze/landing/metadata"


def test_environment_differ_only_by_catalog():
    dev, prod = CatalogNames("cat_dev"), CatalogNames("cat_prod")
    for attr in ("prices_raw", "metadata_raw", "landing_prices_dir", "landing_metadata_dir"):
        assert getattr(dev, attr).replace("cat_dev", "X") == getattr(prod, attr).replace("cat_prod", "X")


@pytest.mark.parametrize("bad", ["", "has.dot", "has space", "Upper", "1starts_with_digit"])
def test_invalid_catalog_is_rejected(bad):
    with pytest.raises(ValueError, match="Invalid catalog name"):
        CatalogNames(bad)


def test_catalog_cannot_change_after_creation():
    names = CatalogNames("my_catalog")
    with pytest.raises(dataclasses.FrozenInstanceError):
        names.catalog = "other"  # type: ignore[misc]


def test_no_catalog_name_is_hardcoded_in_src():
    src = Path(__file__).resolve().parents[2] / "src"
    pattern = re.compile(r"databricks_stock_market_(dev|test|prod)")
    offenders = [str(p) for p in src.rglob("*.py") if pattern.search(p.read_text())]
    assert offenders == []
