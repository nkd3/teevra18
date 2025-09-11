Dim shell: Set shell = CreateObject("WScript.Shell")
shell.Run "powershell.exe -NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File ""C:\teevra18\ops\orchestrator.ps1"" -Mode INTRADAY", 0, True
