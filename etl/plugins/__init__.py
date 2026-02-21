# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

"""Plugin package namespace."""

from .base import (
    PluginContext,
    PluginDefinition,
    PluginLoadError,
    PluginMeta,
    describe_plugin,
    discover_plugins,
    load_plugin,
)

__all__ = [
    "PluginContext",
    "PluginDefinition",
    "PluginLoadError",
    "PluginMeta",
    "describe_plugin",
    "discover_plugins",
    "load_plugin",
]
