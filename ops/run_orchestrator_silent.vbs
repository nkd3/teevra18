' Launch orchestrator.ps1 completely hidden
Dim shell, ps
ps = "powershell.exe -NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File ""C:\teevra18\ops\orchestrator.ps1"" -Mode AUTO"
Set shell = CreateObject("WScript.Shell")
shell.Run ps, 0, True  ' 0 = hidden, True = wait
