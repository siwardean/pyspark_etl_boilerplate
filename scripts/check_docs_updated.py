"""Fail when code changes are not accompanied by a documentation change.

Used by CI on pull requests:  python scripts/check_docs_updated.py origin/main
"""

import subprocess
import sys

#: A change under one of these requires a documentation change.
CODE_PATHS = ("jobs/", "utils/", "etl/", "config/", "main.py")
#: A change under one of these counts as a documentation change.
DOC_PATHS = ("openwiki/", "README.md")


def missing_docs(changed_files):
    """Return the code files that require docs, or [] when the change is fine."""
    code = [path for path in changed_files if path.startswith(CODE_PATHS)]
    documented = any(path.startswith(DOC_PATHS) for path in changed_files)
    return [] if documented else code


def changed_files(base):
    output = subprocess.run(
        ["git", "diff", "--name-only", f"{base}...HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [line for line in output.splitlines() if line]


def main(argv):
    base = argv[1] if len(argv) > 1 else "origin/main"
    undocumented = missing_docs(changed_files(base))
    if undocumented:
        print("These files changed without a documentation update:")
        for path in undocumented:
            print(f"  - {path}")
        print("\nUpdate README.md or a page under openwiki/ in the same pull request.")
        return 1
    print("Documentation check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
