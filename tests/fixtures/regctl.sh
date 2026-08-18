#!/usr/bin/env bash
# Fake `regctl` used by the image-export tests: logs every invocation's argv,
# answers `head` probes, and stores blobs/manifests by digest under
# `{dir}/state`, where `{dir}` is the directory the test symlinked this script
# into (so `$0` still points at the per-test temp dir).
#
# It lives in the repository (mode 100755) rather than being written out by the
# test: a thread holding a write fd on an executable file makes every
# concurrent fork in the same process inherit that fd, and the execve that
# follows then fails with ETXTBSY. Symlinking a shipped file means no thread
# ever opens it for writing, so the window cannot exist.
dir="$(cd "$(dirname "$0")" && pwd)"; state="$dir/state"
printf 'argv:%s\n' "$*" >>"$dir/log"
nf() { echo "regctl: $* [http 404]: not found" >&2; exit 1; }
mkey() { printf '%s' "$1" | tr '/:@' '___'; }
case "$1 $2" in
"blob head") [ -f "$state/blobs/$3/$4" ] || nf "blob $3 $4" ;;
"blob get") [ -f "$state/blobs/$3/$4" ] || nf "blob $3 $4"; cat "$state/blobs/$3/$4" ;;
"blob put") mkdir -p "$state/blobs/$3"; cat >"$state/blobs/$3/$5" ;;
"blob copy") [ -f "$state/blobs/$3/$5" ] || nf "blob $3 $5"; mkdir -p "$state/blobs/$4"; cp "$state/blobs/$3/$5" "$state/blobs/$4/$5" ;;
"manifest head") m="$state/manifests/$(mkey "$3")"; [ -f "$m/digest" ] || nf "manifest $3"; cat "$m/digest" ;;
"manifest put") m="$state/manifests/$(mkey "$5")"; mkdir -p "$m"; cat >"$m/body"; printf 'sha256:%s\n' "$(sha256sum "$m/body" | cut -d' ' -f1)" >"$m/digest" ;;
*) echo "unexpected regctl invocation: $*" >&2; exit 2 ;;
esac
