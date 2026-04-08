#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="$ROOT_DIR/submission"
mkdir -p "$OUT_DIR"

STAMP="$(date +%Y%m%d_%H%M%S)"
LIGHT_ZIP="$OUT_DIR/aicontroller_submission_light_${STAMP}.zip"
FULL_ZIP="$OUT_DIR/aicontroller_submission_full_${STAMP}.zip"

# Light bundle: recommended for class portal upload.
zip -r "$LIGHT_ZIP" \
  README.md requirements.txt Dockerfile docker-compose.yml Makefile \
  src reports .github \
  outputs/metrics outputs/figures outputs/predictions outputs/models \
  data/raw/data_dictionary.md \
  -x "*/__pycache__/*" "*.pyc" "*.DS_Store" "outputs/**/.gitkeep"

# Full bundle: includes generated datasets for maximum reproducibility.
zip -r "$FULL_ZIP" \
  README.md requirements.txt Dockerfile docker-compose.yml Makefile \
  src reports .github \
  data/raw data/processed \
  outputs \
  -x "*/__pycache__/*" "*.pyc" "*.DS_Store" "**/.gitkeep"

echo "Created bundles:"
echo "$LIGHT_ZIP"
echo "$FULL_ZIP"
