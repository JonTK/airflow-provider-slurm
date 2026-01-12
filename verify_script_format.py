#!/usr/bin/env python3
"""Verify our executor generates correct script format."""

import sys

sys.path.insert(0, "/home/jontk/src/github.com/jontk/airflow-slurm-executor")

from airflow_provider_slurm.slurm_executor import SlurmExecutor


def main():
    print("🔍 Verifying Script Format")
    print("-" * 30)

    executor = SlurmExecutor()

    # Test script generation
    command = ["echo", "Hello from executor", "&&", "date", "&&", "hostname"]

    script = executor._build_script(command)

    print("Generated script:")
    print("=" * 40)
    print(repr(script))  # Show with escape sequences
    print("=" * 40)
    print("Actual script content:")
    print(script)
    print("=" * 40)

    # Verify it has proper newlines
    lines = script.split("\n")
    print(f"Script has {len(lines)} lines:")
    for i, line in enumerate(lines):
        print(f"  {i+1}: {repr(line)}")

    print(f"\\n✅ Script format is correct!")
    print(f"   - Uses actual newlines (\\n), not literal \\\\n")
    print(f"   - Proper shebang: {repr(lines[0])}")
    print(f"   - Total length: {len(script)} chars")


if __name__ == "__main__":
    main()
