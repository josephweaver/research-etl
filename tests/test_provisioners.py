from __future__ import annotations

import sys
from etl.provisioners import LocalProvisioner, ProvisionHandle, ProvisionState, SlurmProvisioner, WorkloadSpec


class _FakeTransport:
    def __init__(self) -> None:
        self.put_calls: list[tuple[str, str]] = []
        self.run_calls: list[list[str]] = []

    def put_text(self, text, destination, **_kwargs):
        self.put_calls.append((str(destination), str(text)))
        return type("PutResult", (), {"ok": True, "details": ""})()

    def run(self, argv, **_kwargs):
        self.run_calls.append([str(x) for x in argv])
        if argv[:1] == ["sbatch"]:
            return type("CommandResult", (), {"returncode": 0, "stdout": "Submitted batch job 321\n", "stderr": ""})()
        if argv[:1] == ["squeue"]:
            return type("CommandResult", (), {"returncode": 0, "stdout": "RUNNING\n", "stderr": ""})()
        if argv[:1] == ["scancel"]:
            return type("CommandResult", (), {"returncode": 0, "stdout": "", "stderr": ""})()
        return type("CommandResult", (), {"returncode": 0, "stdout": "", "stderr": ""})()


def test_slurm_provisioner_stages_script_and_submits_job() -> None:
    transport = _FakeTransport()
    provisioner = SlurmProvisioner(transport)

    handle = provisioner.submit(
        WorkloadSpec(
            name="setup",
            script_text="#!/bin/bash\necho ok\n",
            backend_options={
                "destination_dir": "/tmp/work",
                "file_name": "etl-run1-setup.sbatch",
                "dependencies": ["100"],
            },
        )
    )

    assert handle.backend_run_id == "321"
    assert handle.job_ids == ["321"]
    assert transport.put_calls[0][0] == "/tmp/work/etl-run1-setup.sbatch"
    assert transport.run_calls[0] == ["sbatch", "--dependency=afterok:100", "/tmp/work/etl-run1-setup.sbatch"]


def test_slurm_provisioner_maps_scheduler_status_and_cancel() -> None:
    transport = _FakeTransport()
    provisioner = SlurmProvisioner(transport)
    handle = ProvisionHandle(provisioner="slurm", backend_run_id="321", job_ids=["321"])

    status = provisioner.status(handle)
    cancelled = provisioner.cancel(handle)

    assert status.state == ProvisionState.RUNNING
    assert cancelled.state == ProvisionState.CANCELLED
    assert ["squeue", "-h", "-j", "321", "-o", "%T"] in transport.run_calls
    assert ["scancel", "321"] in transport.run_calls


def test_local_provisioner_runs_command_synchronously(tmp_path) -> None:
    out_file = tmp_path / "done.txt"
    provisioner = LocalProvisioner()

    handle = provisioner.submit(
        WorkloadSpec(
            name="local-sync",
            command=[
                sys.executable,
                "-c",
                f"from pathlib import Path; Path(r'{out_file}').write_text('ok', encoding='utf-8')",
            ],
            cwd=str(tmp_path),
        )
    )
    status = provisioner.status(handle)

    assert out_file.read_text(encoding="utf-8") == "ok"
    assert status.state == ProvisionState.SUCCEEDED
    assert status.metadata["returncode"] == 0


def test_local_provisioner_runs_script_text_synchronously(tmp_path) -> None:
    out_file = tmp_path / "script.txt"
    provisioner = LocalProvisioner()

    handle = provisioner.submit(
        WorkloadSpec(
            name="local-script",
            script_text=f"Set-Content -Path '{out_file}' -Value 'script'",
            cwd=str(tmp_path),
            backend_options={"shell": "powershell"},
        )
    )
    status = provisioner.status(handle)

    assert out_file.read_text(encoding="utf-8").strip() == "script"
    assert status.state == ProvisionState.SUCCEEDED


def test_local_provisioner_detached_status_and_cancel() -> None:
    provisioner = LocalProvisioner()

    handle = provisioner.submit(
        WorkloadSpec(
            name="local-detached",
            command=[sys.executable, "-c", "import time; time.sleep(5)"],
            backend_options={"detach": True},
        )
    )
    running = provisioner.status(handle)
    cancelled = provisioner.cancel(handle)
    after = provisioner.status(handle)

    assert running.state == ProvisionState.RUNNING
    assert cancelled.state == ProvisionState.CANCELLED
    assert after.state == ProvisionState.CANCELLED
