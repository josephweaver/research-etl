# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from datetime import datetime

from etl.variable_solver import VariableSolver


def test_variable_solver_overlay_adds_namespace_and_flat_keys() -> None:
    solver = VariableSolver(max_passes=10)
    solver.overlay("globals", {"workdir": "/g/work", "basedir": "/g"})
    ctx = solver.resolved_context()
    assert ctx["workdir"] == "/g/work"
    assert ctx["globals"]["workdir"] == "/g/work"
    assert ctx["basedir"] == "/g"


def test_variable_solver_overlay_precedence_and_jit_get() -> None:
    solver = VariableSolver(max_passes=20)
    solver.overlay("globals", {"workdir": "/global/work"})
    solver.overlay("env", {"workdir": "{globals.workdir}/hpcc"})
    solver.overlay("pipe", {"name": "yanroy", "workdir": "{env.workdir}/{pipe.name}"})
    solver.overlay("cli", {"workdir": "{pipe.workdir}/dev"}, add_namespace=True, add_flat=True)

    assert solver.get("globals.workdir") == "/global/work"
    assert solver.get("env.workdir") == "/global/work/hpcc"
    assert solver.get("pipe.workdir") == "/global/work/hpcc/yanroy"
    assert solver.get("workdir") == "/global/work/hpcc/yanroy/dev"


def test_variable_solver_resolve_complex_values_iteratively() -> None:
    solver = VariableSolver(max_passes=20)
    solver.overlay("globals", {"base": "/data"})
    solver.overlay("env", {"root": "{globals.base}/env"})
    solver.overlay("pipe", {"a": "{env.root}", "b": "{pipe.a}/b"})

    payload = {
        "one": "{pipe.a}",
        "two": "{pipe.b}",
        "nested": {"x": "{env.root}", "y": ["{globals.base}", "{pipe.b}"]},
    }
    resolved = solver.resolve(payload)
    assert resolved["one"] == "/data/env"
    assert resolved["two"] == "/data/env/b"
    assert resolved["nested"]["x"] == "/data/env"
    assert resolved["nested"]["y"] == ["/data", "/data/env/b"]


def test_variable_solver_with_sys_namespace_only() -> None:
    solver = VariableSolver(max_passes=10)
    solver.overlay("globals", {"base": "/tmp"})
    solver.with_sys({"run": {"short_id": "abc12345"}})
    out = solver.resolve("{globals.base}/{sys.run.short_id}")
    assert out == "/tmp/abc12345"
    # sys keys are namespaced only, not flattened.
    assert solver.get("run.short_id", default=None) is None


def test_variable_solver_get_path_uses_env_path_style() -> None:
    solver = VariableSolver(max_passes=20)
    solver.overlay("env", {"path_style": "windows"})
    solver.overlay("pipe", {"workdir": "C:/data/run"})
    assert solver.get_path("pipe.workdir") == r"C:\data\run"

    solver2 = VariableSolver(max_passes=20)
    solver2.overlay("env", {"path_style": "unix"})
    solver2.overlay("pipe", {"workdir": r"C:\data\run"})
    assert solver2.get_path("pipe.workdir") == "C:/data/run"


def test_variable_solver_expr_range_returns_list_for_exact_placeholder() -> None:
    solver = VariableSolver(max_passes=20)
    payload = {"years": "{expr.range(2000, 2002)}"}
    resolved = solver.resolve(payload)
    assert resolved["years"] == [2000, 2001, 2002]


def test_variable_solver_expr_date_nested_and_format_alias() -> None:
    solver = VariableSolver(max_passes=20)
    out = solver.resolve("{expr.dateformat(expr.datediff(expr.date(2020, 1, 31), -1, 'M'), '%YYYY-%MM-%DD')}")
    assert out == "2019-12-31"


def test_variable_solver_expr_dateformat_accepts_sys_now_dict() -> None:
    solver = VariableSolver(max_passes=20)
    solver.with_sys({"now": {"iso_utc": "2026-02-19T15:04:05Z"}})
    out = solver.resolve("{expr.dateformat(sys.now, '%Y%m%d-%H%M%S')}")
    assert out == datetime(2026, 2, 19, 15, 4, 5).strftime("%Y%m%d-%H%M%S")


def test_variable_solver_expr_daterange_returns_daily_strings() -> None:
    solver = VariableSolver(max_passes=20)
    payload = {"days": "{expr.daterange(expr.date(2025,1,1), expr.date(2025,1,3))}"}
    resolved = solver.resolve(payload)
    assert resolved["days"] == ["20250101", "20250102", "20250103"]
