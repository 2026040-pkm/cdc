@echo off
chcp 65001 > nul
cd /d "%~dp0"
echo ============================================
echo  LiDAR sensor data publisher  ^(EMQX 1884^)
echo  350 devices / 22 topics / approx 426 msg/s
echo  Press Ctrl+C to stop.
echo ============================================
echo.
dotnet run lidar-sim.cs -- --host localhost --port 1884
pause
