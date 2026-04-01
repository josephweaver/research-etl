# Windows Environment Notes

Use this file for Windows-specific issues that affect authoring, local testing, or
command execution in this repo.

## Current Notes

### Python interpreter ambiguity

- `python` may resolve to Anaconda instead of the repo `.venv`.
- For dependency-sensitive local commands, prefer:

```powershell
.venv\Scripts\python.exe
```

### User environment variables may not appear in the current shell

Observed with:

- `GCS_HMAC_KEY`
- `GCS_HMAC_SECRET`

If needed for a one-off PowerShell command:

```powershell
$env:GCS_HMAC_KEY = [Environment]::GetEnvironmentVariable('GCS_HMAC_KEY','User')
$env:GCS_HMAC_SECRET = [Environment]::GetEnvironmentVariable('GCS_HMAC_SECRET','User')
```

- Do not assume Windows user-environment edits are visible to the current shell without a fresh process.
