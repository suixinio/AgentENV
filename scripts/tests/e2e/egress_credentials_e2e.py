#!/usr/bin/env python3
"""Runs one shell command inside an existing sandbox through the E2B python SDK.

Usage: egress_credentials_e2e.py run <sandbox_id> <command>

Environment: E2B_API_URL, E2B_SANDBOX_URL, E2B_API_KEY, the same variables
e2b_python_sdk_compat.py reads. Stdout is the command's stdout; the exit
status is the command's exit status.
"""

import os
import sys

from e2b import Sandbox


def main() -> int:
    if len(sys.argv) != 4 or sys.argv[1] != "run":
        print(__doc__, file=sys.stderr)
        return 2
    sandbox_id, command = sys.argv[2], sys.argv[3]
    sandbox = Sandbox.connect(
        sandbox_id,
        api_key=os.environ["E2B_API_KEY"],
        api_url=os.environ["E2B_API_URL"],
        sandbox_url=os.environ["E2B_SANDBOX_URL"],
        request_timeout=60,
    )
    result = sandbox.commands.run(command, timeout=60, request_timeout=90)
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
