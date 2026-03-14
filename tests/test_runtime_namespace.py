from __future__ import annotations

from etl.runtime_context import RuntimeNamespace


def test_runtime_namespace_promotes_dynamic_outputs_to_flat_and_state_namespaces() -> None:
    ns = RuntimeNamespace.from_context({"jobname": "sample"}, max_passes=2)

    ns.set_output("prepared", {"root": "/tmp/data"})

    snap = ns.snapshot()
    assert snap["prepared"] == {"root": "/tmp/data"}
    assert snap["state"]["prepared"] == {"root": "/tmp/data"}


def test_runtime_namespace_resolves_values_against_current_snapshot_without_self_resolving_context() -> None:
    ns = RuntimeNamespace.from_context({"a": "{b}", "b": "ok"}, max_passes=1)

    assert ns.snapshot()["a"] == "{b}"
    assert ns.resolve("{a}") == "{b}"
