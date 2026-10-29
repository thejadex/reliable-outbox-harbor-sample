#!/bin/sh
set -eu
cp /solution/reference_outbox.py /app/outbox.py
chown agent:agent /app/outbox.py
