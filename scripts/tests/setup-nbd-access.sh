#!/usr/bin/env bash
# Prepare a test host for the nbd block transport: load the in-tree nbd module
# and give the runtime group read/write access to /dev/nbd*. The netlink side
# needs CAP_SYS_ADMIN, which scripts/run-with-capabilities.sh grants.

set -euo pipefail

if (($# != 2)); then
    echo "usage: $0 <user> <group>" >&2
    exit 2
fi

if [[ ${EUID} -ne 0 ]]; then
    echo "error: nbd host setup must run as root" >&2
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
    echo "error: invalid or unknown nbd device group: ${group}" >&2
    exit 1
fi
if [[ "$(getent group "$group" | cut -d: -f3)" == "0" ]]; then
    echo "error: nbd device group must be non-root" >&2
    exit 1
fi

modprobe nbd
test -d /sys/module/nbd

install -d -m 0755 /etc/udev/rules.d
printf '%s\n' \
    '# Managed by AENV CI' \
    "KERNEL==\"nbd*\", MODE=\"0660\", GROUP=\"${group}\"" \
    > /etc/udev/rules.d/99-agentenv-nbd.rules
udevadm control --reload-rules
udevadm trigger --subsystem-match=block --sysname-match='nbd*'
udevadm settle --timeout=10

setpriv --reuid="$user" --regid="$group" --init-groups sh -c '
    test -r /dev/nbd0 && test -w /dev/nbd0
'
