.venv/Scripts/Activate.ps1
pip install -e ".[web]"
etl web --host 127.0.0.1 --port 8000 --reload