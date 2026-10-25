"""Offline checks on databricks.yml: each target writes where its name says, in the right mode.

Only the offline half of deploy provenance is checked. The workflow half (prod deploys the SHA
the test target proved) does not exist yet: there is no deploy workflow, and on Free Edition there
are no service principals, so a CI deploy would need a long-lived personal token (issue #9).

`databricks bundle validate` needs credentials, so the properties that can be read straight off
databricks.yml are asserted here and run on every push.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
BUNDLE = REPO_ROOT / "databricks.yml"
RESOURCES = REPO_ROOT / "resources"

EXPECTED_CATALOG = {
    "dev": "databricks_stock_market_dev",
    "test": "databricks_stock_market_test",
    "prod": "databricks_stock_market_prod",
}


def load_bundle() -> dict[str, Any]:
    document: dict[str, Any] = yaml.safe_load(BUNDLE.read_text(encoding="utf-8"))
    return document


def test_every_target_declares_a_catalog_matching_its_environment() -> None:
    targets = load_bundle()["targets"]

    assert set(targets) == set(EXPECTED_CATALOG), (
        f"databricks.yml declares targets {sorted(targets)}, expected {sorted(EXPECTED_CATALOG)}"
    )
    for name, target in targets.items():
        variables = target.get("variables") or {}
        assert variables.get("catalog") == EXPECTED_CATALOG[name], (
            f"target {name} declares catalog {variables.get('catalog')!r}, expected {EXPECTED_CATALOG[name]!r}"
        )


def test_catalog_variable_has_no_default() -> None:
    """A default would let a target that forgets to set it silently write into that catalog."""
    catalog = load_bundle()["variables"]["catalog"]
    assert "default" not in catalog, f"var.catalog has a default ({catalog['default']!r}); every target must set it"


def test_promotion_targets_are_production_mode() -> None:
    """`test` and `prod` are `mode: production`; only `dev` is a development target.

    Under `mode: development` a target gets its resources renamed, its pipelines forced into
    development semantics (compute reuse, no retries) and its schedules paused, so what passed on
    test would not be the deployment prod receives. `dev` stays development mode deliberately: it
    is the interactive loop, and production mode's refusal of a dirty tree is wrong there.
    """
    targets = load_bundle()["targets"]
    assert targets["dev"]["mode"] == "development"
    for name in ("test", "prod"):
        assert targets[name]["mode"] == "production", f"target {name} is {targets[name]['mode']!r}"


def test_no_pipeline_resource_pins_development_mode() -> None:
    """The target decides `development`, not the resource.

    A hardcoded `development: true` makes `bundle validate -t prod` fail ("target with
    'mode: production' cannot include a pipeline with 'development: true'").
    """
    for resource_file in sorted(RESOURCES.glob("*.yml")):
        document = yaml.safe_load(resource_file.read_text(encoding="utf-8")) or {}
        pipelines = (document.get("resources") or {}).get("pipelines") or {}
        for name, pipeline in pipelines.items():
            assert "development" not in pipeline, (
                f"{resource_file.name}:{name} pins `development: {pipeline['development']}`. Let the bundle mode set it."
            )


def test_catalogs_are_not_bundle_resources() -> None:
    """Free Edition rejects catalog creation through the REST API the bundle uses (DL-003/DL-004)."""
    for resource_file in sorted(RESOURCES.glob("*.yml")):
        document = yaml.safe_load(resource_file.read_text(encoding="utf-8")) or {}
        assert "catalogs" not in (document.get("resources") or {}), (
            f"{resource_file.name} declares a catalogs resource; catalogs are created by scripts/bootstrap_catalogs.py"
        )
