@echo off
title Flash Sale Harbolnas PoC - Kelompok 7 APL UNY 2025
cls

echo ======================================================================
echo     🔴 SIMULATOR FLASH SALE HARBOLNAS - POC ARSITEKTUR v1.0
echo             Studi Kasus 7 | Kelompok 7 | APL UNY 2025
echo ======================================================================
echo.
echo [INFO] Menyiapkan lingkungan sistem...
echo.

:: 1. Check if Node.js is installed
where node >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Node.js tidak terdeteksi pada sistem Anda!
    echo Silakan install Node.js versi 18 atau lebih tinggi dari https://nodejs.org/
    echo.
    pause
    exit /b
)

:: 2. Install npm dependencies
echo [1/2] Menginstal dependensi npm secara otomatis (chalk, cli-table3)...
echo ----------------------------------------------------------------------
call npm install
echo ----------------------------------------------------------------------
echo [SUKSES] Seluruh komponen dependensi berhasil disiapkan!
echo.

:: 3. Run the application
echo [2/2] Memulai server web lokal dan terminal logger...
echo ----------------------------------------------------------------------
echo [INFO] Jika selesai, Anda dapat menutup jendela ini untuk mematikan server.
echo.
call npm start

pause
