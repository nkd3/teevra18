Set sh = CreateObject("WScript.Shell")
cmd = "powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File ""C:\teevra18\services\m11\run_oos_capture_once.ps1"""
sh.Run cmd, 0, False
