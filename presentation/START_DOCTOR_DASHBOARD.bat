@echo off
setlocal
cd /d "%~dp0"
set "IPPR=%USERPROFILE%\Desktop\BigData_Offline_Tools\01_IPPR_Environment\IPPR_RAW_COPY\python.exe"
if not exist "%IPPR%" (
  echo [ERROR] Python environment not found:
  echo %IPPR%
  pause
  exit /b 1
)
"%IPPR%" "doctor_dashboard_server.py"
pause
