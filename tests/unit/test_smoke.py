"""The smoke entry point: what the wheel-install job prints to stdout."""

import stock_market
from stock_market import smoke


def test_main_reports_package_and_dependency_versions(capsys):
    smoke.main()

    out = capsys.readouterr().out
    assert f"stock_market {stock_market.__version__}" in out
    assert "pandas" in out
    assert "pyarrow" in out
