# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import pytest

from etl.datasets.routing import (
    DatasetRoutingError,
    default_location_type,
    infer_transport,
    validate_target,
)


def test_infer_transport_local_to_filesystem() -> None:
    transport = infer_transport(
        runtime_context="local",
        target_location_type="local_cache",
        policy={"locations": {"local_cache": {"kind": "filesystem"}}},
    )
    assert transport == "local_fs"


def test_infer_transport_to_gdrive_uses_rclone() -> None:
    transport = infer_transport(
        runtime_context="local",
        target_location_type="gdrive",
        policy={"locations": {"gdrive": {"kind": "gdrive"}}},
    )
    assert transport == "rclone"


def test_infer_transport_to_gcs_uses_gcs() -> None:
    transport = infer_transport(
        runtime_context="local",
        target_location_type="gcs",
        policy={"locations": {"gcs": {"kind": "gcs"}}},
    )
    assert transport == "gcs"


def test_default_location_type_staging_local() -> None:
    assert default_location_type(stage="staging", runtime_context="local", policy=None) == "local_cache"


def test_validate_target_rejects_disallowed_location() -> None:
    with pytest.raises(DatasetRoutingError):
        validate_target(
            policy={
                "classes": {"cache": {"allowed_location_types": ["hpcc_cache"]}},
                "locations": {"local_cache": {"kind": "filesystem", "root_path": "/tmp/cache"}},
            },
            artifact_class="cache",
            location_type="local_cache",
            target_uri="/tmp/cache/x.bin",
        )
