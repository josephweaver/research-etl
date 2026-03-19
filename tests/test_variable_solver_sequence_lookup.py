from __future__ import annotations

from etl.expr import eval_expr_text
from etl.variable_solver import VariableSolver


def test_variable_solver_lookup_path_supports_sequence_indices() -> None:
    ctx = {
        "item": [2000, "MI"],
        "pairs": [
            [2000, "MI"],
            [2001, "WI"],
        ],
    }

    year, ok = VariableSolver._lookup_path(ctx, "item.0")
    state, ok_state = VariableSolver._lookup_path(ctx, "item.1")
    nested, ok_nested = VariableSolver._lookup_path(ctx, "pairs.1.1")

    assert ok is True
    assert ok_state is True
    assert ok_nested is True
    assert year == 2000
    assert state == "MI"
    assert nested == "WI"


def test_expr_lookup_path_supports_sequence_indices() -> None:
    ctx = {"item": [2000, "MI"]}

    assert eval_expr_text("expr.join(item, '-')", ctx) == "2000-MI"
