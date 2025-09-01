Set sh = CreateObject("WScript.Shell")
cmd = "powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File ""C:\teevra18\services\m11\run_reinfer_once.ps1"""
sh.Run cmd, 0, False
