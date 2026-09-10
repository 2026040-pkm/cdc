@echo off
chcp 65001 > nul
cd /d "%~dp0"
echo ============================================
echo  LiDAR publisher - LOW RATE mode
echo  status/scan both 10 min  =^> approx 8.2 msg/s
echo  Press Ctrl+C to stop.
echo ============================================
echo.
dotnet run lidar-sim.cs -- --host localhost --port 1884 --status-interval 600000 --interval 600000
pause
