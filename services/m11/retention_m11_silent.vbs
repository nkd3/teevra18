Set sh = CreateObject("WScript.Shell")
cmd = "powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -Command ""$env:DB_PATH='C:\teevra18\data\teevra18.db'; & 'C:\teevra18\.venv\Scripts\Activate.ps1'; python 'C:\teevra18\services\m11\retention_m11.py'"""
sh.Run cmd, 0, False
