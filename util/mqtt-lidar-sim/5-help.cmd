@echo off
chcp 65001 > nul
cd /d "%~dp0"
dotnet run lidar-sim.cs -- --help
pause
