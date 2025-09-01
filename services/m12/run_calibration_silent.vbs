Set sh = CreateObject("WScript.Shell")
cmd = "powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File ""C:\teevra18\services\m12\run_calibration_once.ps1"""
sh.Run cmd, 0, False
