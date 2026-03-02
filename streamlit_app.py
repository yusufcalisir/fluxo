import os
import sys

# Add the local src directory to Python's import path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "src", "fluxo", "ui", "app.py")

# Execute the Streamlit UI script natively
with open(SCRIPT_PATH, "r", encoding="utf-8") as f:
    code = compile(f.read(), SCRIPT_PATH, 'exec')
    exec(code, {'__file__': SCRIPT_PATH})
