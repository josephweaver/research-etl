# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

from etl.plugins.base import PluginContext, load_plugin


def _ctx(tmp_path: Path) -> PluginContext:
    return PluginContext(run_id="r1", workdir=tmp_path, log=lambda *a, **k: None)


def test_terminate_plugin_raises_runtime_error(tmp_path: Path) -> None:
    plugin = load_plugin(Path("plugins/terminate.py"))
    try:
        plugin.run({"reason": "stop now"}, _ctx(tmp_path))
        assert False, "expected terminate plugin to raise"
    except RuntimeError as exc:
        assert "stop now" in str(exc)
