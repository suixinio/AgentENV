#!/bin/sh
# Fake `regctl` used by the image-resolver tests: records every argument (one
# per line) into `{dir}/argv`, replays `{dir}/stdout` and `{dir}/stderr`, and
# exits with the status in `{dir}/exit_code`. `{dir}` is the directory the test
# symlinked this script into, so `$0` still points at the per-test temp dir and
# no absolute path is baked in -- a TMPDIR containing shell metacharacters can
# neither break nor inject into it.
#
# It lives in the repository (mode 100755) rather than being written out by the
# test: a thread holding a write fd on an executable file makes every concurrent
# fork in the same process inherit that fd, and the execve that follows then
# fails with ETXTBSY. Symlinking a shipped file means no thread ever opens it
# for writing, so the window cannot exist.
dir="$(cd "$(dirname "$0")" && pwd)"
printf '%s\n' "$@" > "$dir/argv"
cat "$dir/stdout"
cat "$dir/stderr" >&2
exit "$(cat "$dir/exit_code")"
