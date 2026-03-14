from __future__ import annotations

from etl.runtime_context import (
    eval_runtime_when,
    lookup_runtime_value,
    resolve_runtime_expr_only,
    resolve_runtime_template_text,
    resolve_runtime_value,
)


def test_lookup_runtime_value_supports_dotted_paths() -> None:
    value, ok = lookup_runtime_value({"sys": {"run": {"id": "abc"}}}, "sys.run.id")
    assert ok is True
    assert value == "abc"


def test_resolve_runtime_value_handles_placeholders_and_expr() -> None:
    ctx = {"a": "ok", "sys": {"run": {"short_id": "1234"}}}
    assert resolve_runtime_template_text("x={a}-{sys.run.short_id}", ctx) == "x=ok-1234"
    assert resolve_runtime_value("x={a}-{sys.run.short_id}", ctx) == "x=ok-1234"
    assert resolve_runtime_value("{expr.range(1,3)}", ctx) == [1, 2, 3]


def test_resolve_runtime_expr_only_leaves_non_expr_placeholders_intact() -> None:
    ctx = {"a": "{b}", "b": "ok"}
    assert resolve_runtime_expr_only("{a}", ctx, max_passes=1) == "{a}"
    assert resolve_runtime_expr_only("{expr.join(['a','b'], sep='-')}", ctx) == "a-b"


def test_eval_runtime_when_matches_existing_runner_behavior() -> None:
    assert eval_runtime_when("x == 1", {"x": 1}) is True
    assert eval_runtime_when("x == 2", {"x": 1}) is False
    assert eval_runtime_when("missing == 1", {"x": 1}) is False
