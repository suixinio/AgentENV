#!/bin/sh
# Fake Docker credential helper used by the ACR client tests.
#
# It lives in the repository (mode 100755) instead of being written out by the
# test: a test thread that holds a write fd on an executable file makes every
# concurrent fork in the same process inherit that fd, and the execve that
# follows then fails with ETXTBSY. Shipping the file means no thread ever
# opens it for writing, so the window cannot exist.
read _server
printf '{"Username":"helper-user","Secret":"helper-secret"}'
