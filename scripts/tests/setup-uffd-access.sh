#!/usr/bin/env bash
# Prepare a test host for the userfaultfd memory backend: give the runtime
# group read/write access to /dev/userfaultfd, which Firecracker opens to
# create the descriptor it hands the daemon. The handler side needs nothing.

set -euo pipefail

if (($# != 2)); then
    echo "usage: $0 <user> <group>" >&2
    exit 2
fi

if [[ ${EUID} -ne 0 ]]; then
    echo "error: uffd host setup must run as root" >&2
    exit 1
fi

user="$1"
group="$2"

if [[ ! "$user" =~ ^[a-z_][a-z0-9_-]*$ ]] || ! id -u "$user" >/dev/null 2>&1; then
    echo "error: invalid or unknown runtime user: ${user}" >&2
    exit 1
fi
if [[ "$(id -u "$user")" == "0" ]]; then
    echo "error: runtime user must be non-root" >&2
    exit 1
fi
if [[ ! "$group" =~ ^[a-z_][a-z0-9_-]*$ ]] || ! getent group "$group" >/dev/null; then
    echo "error: invalid or unknown device group: ${group}" >&2
    exit 1
fi
if [[ "$(getent group "$group" | cut -d: -f3)" == "0" ]]; then
    echo "error: device group must be non-root" >&2
    exit 1
fi

if [[ ! -e /dev/userfaultfd ]]; then
    echo "no /dev/userfaultfd on this kernel; Firecracker falls back to userfaultfd(2), which needs CAP_SYS_PTRACE (scripts/run-with-capabilities.sh grants it) or vm.unprivileged_userfaultfd=1"
    exit 0
fi

install -d -m 0755 /etc/udev/rules.d
printf '%s\n' \
    '# Managed by AENV CI' \
    "KERNEL==\"userfaultfd\", MODE=\"0660\", GROUP=\"${group}\"" \
    > /etc/udev/rules.d/99-agentenv-uffd.rules
udevadm control --reload-rules
udevadm trigger --subsystem-match=misc --sysname-match=userfaultfd
udevadm settle --timeout=10

setpriv --reuid="$user" --regid="$group" --init-groups sh -c '
    test -r /dev/userfaultfd && test -w /dev/userfaultfd
'
