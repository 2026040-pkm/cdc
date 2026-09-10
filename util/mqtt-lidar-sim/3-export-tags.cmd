@echo off
chcp 65001 > nul
cd /d "%~dp0"
echo Exporting tag definition CSVs into .\tags ...
dotnet run lidar-sim.cs -- --export-tags tags
echo.
echo Done. Import each CSV in the UI (one file = one topic).
pause
