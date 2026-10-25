"""Offline checks on scripts/bootstrap_catalogs.py: the SQL it would run."""

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "bootstrap_catalogs.py"


@pytest.fixture(scope="module")
def bootstrap():
    spec = importlib.util.spec_from_file_location("bootstrap_catalogs", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["bootstrap_catalogs"] = module
    spec.loader.exec_module(module)
    return module


def test_every_create_is_idempotent(bootstrap):
    for env in bootstrap.ENVIRONMENTS:
        creates = [s for s in bootstrap.bootstrap_statements(env) if s.startswith("CREATE")]
        assert creates
        assert all(" IF NOT EXISTS " in s for s in creates), creates


def test_statements_target_only_their_own_catalog(bootstrap):
    for env in bootstrap.ENVIRONMENTS:
        others = {bootstrap.catalog_name(e) for e in bootstrap.ENVIRONMENTS} - {bootstrap.catalog_name(env)}
        for statement in bootstrap.bootstrap_statements(env):
            assert not any(other in statement for other in others), statement


def test_catalog_is_tagged_with_its_environment(bootstrap):
    tag = bootstrap.bootstrap_statements("prod")[-1]
    assert tag.startswith("ALTER CATALOG databricks_stock_market_prod SET TAGS")
    assert "'environment' = 'prod'" in tag
    assert "'project' = 'stock-market'" in tag


def test_comments_are_quoted_safely(bootstrap):
    assert bootstrap.sql_string("it's") == "'it''s'"
