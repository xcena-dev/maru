#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# monitor_me.sh — periodic dump of /sys/fs/marufs/me_info
#
# Usage:
#   sudo ./monitor_me.sh                    # all ME, 200ms interval
#   sudo ./monitor_me.sh 0.05               # all ME, 50ms interval
#   sudo ./monitor_me.sh 0.1 global         # only Global ME
#   sudo ./monitor_me.sh 0.1 0              # only NRHT region 0
#   sudo ./monitor_me.sh 0.1 all timestamp  # prefix every line with timestamp
#   sudo ./monitor_me.sh 0.1 all diff       # only print when output changes
#
# Requires marufs.ko with me_info sysfs (sysfs.c).

set -u

SYS=/sys/fs/marufs/me_info
INTERVAL="${1:-0.2}"
FILTER="${2:-all}"
MODE="${3:-plain}"     # plain | timestamp | diff

if [ ! -e "$SYS" ]; then
    echo "ERROR: $SYS missing — rebuild + reinstall marufs.ko" >&2
    exit 1
fi

# Select filter (global | <rid> | all)
echo "$FILTER" > "$SYS" || { echo "ERROR: failed to set filter '$FILTER'"; exit 1; }

prev=""
while :; do
    ts="$(date '+%H:%M:%S.%3N')"
    cur="$(cat "$SYS")"

    case "$MODE" in
        diff)
            if [ "$cur" != "$prev" ]; then
                printf '===== %s =====\n%s\n' "$ts" "$cur"
                prev="$cur"
            fi
            ;;
        timestamp)
            printf '%s\n' "$cur" | sed "s|^|$ts  |"
            echo "---"
            ;;
        plain|*)
            clear 2>/dev/null || true
            printf '===== %s (filter=%s) =====\n' "$ts" "$FILTER"
            printf '%s\n' "$cur"
            ;;
    esac
    sleep "$INTERVAL"
done
