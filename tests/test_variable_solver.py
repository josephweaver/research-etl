from __future__ import annotations

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
