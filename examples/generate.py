#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyyaml"]
# ///
"""Generate example notebooks from YAML specs.

Usage:
    uv run examples/generate.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SCRIPT = Path.home() / ".claude/marketplaces/personal/general/skills/nb-yaml/scripts/nb_yaml.py"
SPECS_DIR = Path(__file__).parent / "_specs"


def main() -> None:
    if not SCRIPT.exists():
        print(f"Error: nb_yaml.py not found at {SCRIPT}", file=sys.stderr)
        print("Install the nb-yaml skill first.", file=sys.stderr)
        sys.exit(1)

    if not SPECS_DIR.exists():
        print(f"Error: _specs/ directory not found at {SPECS_DIR}", file=sys.stderr)
        sys.exit(1)

    # Generate .ipynb for each .yaml spec, placing them alongside in the parent dir
    for yaml_file in sorted(SPECS_DIR.rglob("*.yaml")):
        rel = yaml_file.relative_to(SPECS_DIR)
        output_dir = SPECS_DIR.parent / rel.parent
        output_dir.mkdir(parents=True, exist_ok=True)

        subprocess.run(
            [sys.executable, str(SCRIPT), "generate", str(yaml_file), "--output", str(output_dir)],
            check=True,
        )

    print("\nDone! Notebooks generated in examples/")


if __name__ == "__main__":
    main()
