"""
Centralized path constants based on pathlib.

Architectural Assumption:
We use a centralized `paths.py` instead of hardcoding paths in each script to:
1. Prevent execution location errors: `pathlib.Path(__file__)` dynamically calculates 
   the absolute root directory, meaning scripts can be executed from anywhere.
2. Avoid duplication (DRY): If a folder name changes, we only update it here.
3. Keep logic clean: Business logic scripts don't need to worry about OS-level path resolution.
"""
# ES: Constantes de rutas centralizadas usando pathlib.

from pathlib import Path

# Project root resolves two parents above this file (src/paths.py -> src -> ROOT)
# ES: ROOT del proyecto: dos niveles arriba de este archivo
ROOT = Path(__file__).resolve().parents[1]

# Data directories
# ES: Directorios de datos
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"
DATA_STREAMING = ROOT / "data" / "streaming"

# Artifact directories
# ES: Directorios de artefactos
MODELS_DIR = ROOT / "models"
SQL_DIR = ROOT / "sql"
NOTEBOOKS_DIR = ROOT / "notebooks"
