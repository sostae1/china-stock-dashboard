import os, subprocess, json, time
os.chdir(os.path.dirname(os.path.abspath(__file__)))
subprocess.run(["python", "run_zt.py"], capture_output=True)
