"""
This file makes the 'src' directory a Python package.

Architectural Assumption:
It is required so that other scripts in the project (like the notebooks or kafka scripts)
can import modules from this directory without raising a 'ModuleNotFoundError'.
For example: `from src.paths import DATA_RAW`
"""
