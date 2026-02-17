from __future__ import annotations

import cli


def test_cmd_datasets_list_prints_rows(monkeypatch, capsys):
    parser = cli.build_parser()
    args = parser.parse_args(["datasets", "list", "--limit", "5"])
    monkeypatch.setattr(
        cli,
        "list_datasets",
        lambda **kwargs: [
            {
                "dataset_id": "serve.demo",
                "status": "active",
                "data_class": "SERVE",
                "owner_user": "land-core",
                "version_count": 2,
                "latest_version": "v2",
            }
        ],
    )
    rc = cli.cmd_datasets_list(args)
    captured = capsys.readouterr()
    assert rc == 0
    assert "serve.demo" in captured.out
    assert "versions=2" in captured.out


def test_cmd_datasets_show_not_found(monkeypatch, capsys):
    parser = cli.build_parser()
    args = parser.parse_args(["datasets", "show", "serve.missing"])
    monkeypatch.setattr(cli, "get_dataset", lambda dataset_id: None)
    rc = cli.cmd_datasets_show(args)
    captured = capsys.readouterr()
    assert rc == 1
    assert "Dataset not found" in captured.err


def test_cmd_datasets_show_prints_details(monkeypatch, capsys):
    parser = cli.build_parser()
    args = parser.parse_args(["datasets", "show", "serve.demo"])
    monkeypatch.setattr(
        cli,
        "get_dataset",
        lambda dataset_id: {
            "dataset_id": "serve.demo",
            "status": "active",
            "data_class": "SERVE",
            "owner_user": "land-core",
            "created_at": "2026-02-17T01:02:03+00:00",
            "updated_at": "2026-02-17T01:03:03+00:00",
            "versions": [
                {
                    "dataset_version_id": 2,
                    "version_label": "v2",
                    "is_immutable": True,
                    "schema_hash": "abc",
                    "created_by_run_id": "r2",
                    "created_at": "2026-02-17T01:03:03+00:00",
                }
            ],
            "locations": [
                {
                    "dataset_version_id": 2,
                    "version_label": "v2",
                    "environment": "local",
                    "location_type": "local_cache",
                    "uri": "file:///tmp/demo",
                    "is_canonical": True,
                    "checksum": "abc",
                    "size_bytes": 123,
                    "created_at": "2026-02-17T01:03:04+00:00",
                }
            ],
        },
    )
    rc = cli.cmd_datasets_show(args)
    captured = capsys.readouterr()
    assert rc == 0
    assert "Dataset: serve.demo" in captured.out
    assert "Locations:" in captured.out
