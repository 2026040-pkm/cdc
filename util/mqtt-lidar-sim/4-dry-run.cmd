@echo off
chcp 65001 > nul
cd /d "%~dp0"
echo Payload shape only - no broker connection.
dotnet run lidar-sim.cs -- --dry-run --segments 2 --assembly-devices 1 --outfitting-devices 0
pause
