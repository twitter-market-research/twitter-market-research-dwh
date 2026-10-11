
#!/usr/bin/env python3

"""Fail if any infrastructure YAML under iac/ cannot be parsed"

A rule that Prometheus or Compose cannot read is a rule that silently
does not apply - we have to avoid it with this file
"""

import pathlib
import sys

import yaml


def main() -> int:
    """
    Parse every YAML file under iac/ folder

    Returns
    -------
    int
        0 when all files parse, 1 otherwise
    """

    failures = []
    for path in sorted(pathlib.Path("iac").rglob("*.y*ml")):
        try:
            yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exception:
            failures.append(f"{path}: {exception}")

    for line in failures:
        print(line, file=sys.stderr)
    print(f"{'FAIL' if failures else 'OK'} - infra YAML")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
