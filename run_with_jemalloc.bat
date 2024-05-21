@echo off
set "LD_PRELOAD=/mingw64/bin/libjemalloc.so"
python "C:\simple_path\main.py"
pause
