#!/bin/bash
# A probe: mounts one volume twice and prints whether the second mount sees what the first one writes.
set -u
built=$(pwd)/target/release
store=$RUNNER_TEMP/store-two
first=$RUNNER_TEMP/mount-first
second=$RUNNER_TEMP/mount-second
mkdir -p "$first" "$second"
volume=$("$built/stratafs-bench" create "$store")
# FSKit takes a second mount of the same resource for busy, so the second mount reaches the store through a link
ln -s "$store" "$RUNNER_TEMP/store-two-link"
/sbin/mount -F -t stratafs -o "volume=$volume" "$store" "$first"; echo "first mount exit $?"
/sbin/mount -F -t stratafs -o "volume=$volume" "$RUNNER_TEMP/store-two-link" "$second"; echo "second mount exit $?"
echo one > "$first/file"
sleep 2
echo "second reads: $(cat "$second/file" 2>&1)"
ls "$second" > /dev/null
echo two-longer > "$first/file"
echo new > "$first/another"
for wait in 1 3 10; do
  sleep "$wait"
  echo "after ${wait}s more the second reads: $(cat "$second/file" 2>&1), lists: $(ls "$second" | tr '\n' ' ')"
done
[ "$(cat "$second/file")" = two-longer ] && echo "RESULT the second mount saw the first mount's overwrite" || echo "RESULT the second mount kept stale content"
/sbin/umount "$first"; /sbin/umount "$second"
log show --last 3m --style compact --predicate 'eventMessage CONTAINS[c] "setCacheState" OR eventMessage CONTAINS[c] "kernel'"'"'s copy" OR (process == "amfid" AND eventMessage CONTAINS[c] "stratafs")' 2>/dev/null | tail -12 | cut -c1-260
exit 0
