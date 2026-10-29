#!/bin/sh
set -eu
umask 077
mkdir -p /logs/verifier
chmod 700 /logs/verifier /tests
printf '0\n' > /logs/verifier/reward.txt
if python3 -I /tests/checks.py > /logs/verifier/checks.log 2>&1; then
    printf '1\n' > /logs/verifier/reward.txt
fi
cat /logs/verifier/checks.log
