#!/usr/bin/env python3
"""
Automated Flake8 fixes for streaming transformations module.

This script fixes common, low-risk Flake8 issues:
- W291: Trailing whitespace
- W292: No newline at end of file
- W293: Blank line contains whitespace
- F401: Unused imports (safe removals only)
"""

import os
import re
from pathlib import Path


def fix_trailing_whitespace(content: str) -> str:
    """Fix W291 and W293 - trailing whitespace and whitespace-only lines."""
    lines = content.split("\n")
    fixed_lines = []

    for line in lines:
        # Remove trailing whitespace
        fixed_line = line.rstrip()
        fixed_lines.append(fixed_line)

    return "\n".join(fixed_lines)


def fix_file_ending(content: str) -> str:
    """Fix W292 - ensure file ends with newline."""
    if content and not content.endswith("\n"):
        content += "\n"
    return content


def remove_unused_imports(content: str, filepath: str) -> str:
    """Remove clearly unused imports (F401)."""
    # Only remove imports we're confident are unused
    safe_removals = {
        "from pyspark.sql.window import Window": "Window",
    }

    lines = content.split("\n")
    fixed_lines = []

    for line in lines:
        should_remove = False
        for import_line, symbol in safe_removals.items():
            if line.strip() == import_line:
                # Check if symbol is used anywhere in the file
                if symbol not in content.replace(line, ""):
                    should_remove = True
                    print(f"Removing unused import: {import_line} from {filepath}")
                    break

        if not should_remove:
            fixed_lines.append(line)

    return "\n".join(fixed_lines)


def fix_line_length_safe(content: str) -> str:
    """Fix safe line length issues (E501) - only obvious cases."""
    lines = content.split("\n")
    fixed_lines = []

    for line in lines:
        # Only fix very safe line length issues
        if len(line) > 79 and line.strip().endswith(","):
            # Split long parameter lists at commas
            if "(" in line and ")" not in line:
                indent = len(line) - len(line.lstrip())
                if line.count(",") >= 2:
                    # Split at commas for long parameter lists
                    parts = line.split(",")
                    if len(parts) > 1:
                        base_indent = " " * (indent + 4)
                        fixed_line = parts[0] + ","
                        for part in parts[1:-1]:
                            fixed_line += "\n" + base_indent + part.strip() + ","
                        if parts[-1].strip():
                            fixed_line += "\n" + base_indent + parts[-1].strip()
                        fixed_lines.append(fixed_line)
                        continue

        fixed_lines.append(line)

    return "\n".join(fixed_lines)


def fix_streaming_file(filepath: Path) -> None:
    """Fix a single streaming transformation file."""
    print(f"Processing: {filepath}")

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        original_content = content

        # Apply fixes
        content = fix_trailing_whitespace(content)
        content = fix_file_ending(content)
        content = remove_unused_imports(content, str(filepath))

        # Only write if content changed
        if content != original_content:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"✅ Fixed: {filepath}")
        else:
            print(f"ℹ️  No changes needed: {filepath}")

    except Exception as e:
        print(f"❌ Error processing {filepath}: {e}")


def main():
    """Main function to fix all streaming transformation files."""
    streaming_dir = Path("src/streaming/transformations")

    if not streaming_dir.exists():
        print(f"❌ Directory not found: {streaming_dir}")
        return

    # Find all Python files
    python_files = list(streaming_dir.glob("*.py"))

    if not python_files:
        print(f"❌ No Python files found in {streaming_dir}")
        return

    print(f"Found {len(python_files)} Python files to process")
    print("=" * 50)

    for filepath in python_files:
        fix_streaming_file(filepath)
        print()

    print("=" * 50)
    print("Flake8 fixes completed!")
    print(
        "\nRun 'poetry run flake8 src/streaming/transformations/' to check remaining issues."
    )


if __name__ == "__main__":
    main()
