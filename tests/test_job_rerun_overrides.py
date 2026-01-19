from __future__ import annotations

from metrobikeatlas.api.jobs import apply_build_silver_overrides


def test_apply_build_silver_overrides_replaces_value_flags() -> None:
    args = ["--max-availability-files", "500", "--bronze-dir", "data/bronze"]
    out = apply_build_silver_overrides(args, {"max_availability_files": 123, "bronze_dir": "X"})
    assert "--max-availability-files" in out
    assert out[out.index("--max-availability-files") + 1] == "123"
    assert "--bronze-dir" in out
    assert out[out.index("--bronze-dir") + 1] == "X"


def test_apply_build_silver_overrides_toggles_boolean_flag() -> None:
    args = ["--prefer-external-metro"]
    out = apply_build_silver_overrides(args, {"prefer_external_metro": False})
    assert "--prefer-external-metro" not in out
    out2 = apply_build_silver_overrides([], {"prefer_external_metro": True})
    assert "--prefer-external-metro" in out2

