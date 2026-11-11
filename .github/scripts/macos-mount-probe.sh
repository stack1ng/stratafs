#!/bin/bash
# Assembles the benchmark FSKit module signed ad hoc, enables it by editing FSKit's enabled list, and mounts a volume.
# A probe of what a hosted macOS runner allows; it prints what it finds and never fails the job.
set -u
workspace=$(pwd)
sources=$workspace/crates/stratafs-bench/bundle
built=$workspace/target/release
app=$workspace/target/bench/StrataBench.app
appex=$app/Contents/Extensions/StrataBench.appex
lsregister=/System/Library/Frameworks/CoreServices.framework/Versions/Current/Frameworks/LaunchServices.framework/Versions/Current/Support/lsregister
enabled="$HOME/Library/Group Containers/group.com.apple.fskit.settings/enabledModules.plist"
module=dev.stratafs.bench.extension

assemble() {
  entitlements=$1
  rm -rf "$app"
  mkdir -p "$appex/Contents/MacOS" "$app/Contents/MacOS"
  cp "$built/stratafs-bench-extension" "$appex/Contents/MacOS/"
  cp "$sources/extension/Info.plist" "$appex/Contents/Info.plist"
  cp "$built/stratafs-bench-host" "$app/Contents/MacOS/"
  cp "$sources/host/Info.plist" "$app/Contents/Info.plist"
  codesign --force --sign - --timestamp=none --options runtime --entitlements "$entitlements" --generate-entitlement-der "$appex"
  codesign --force --sign - --timestamp=none --options runtime --entitlements "$sources/sandbox.entitlements" --generate-entitlement-der "$app"
  "$lsregister" -f -R -trusted "$app"
  sleep 2
  pluginkit -m -v -p com.apple.fskit.fsmodule
}

enable() {
  cp "$enabled" "$RUNNER_TEMP/enabledModules.before.plist"
  /usr/bin/python3 - "$enabled" "$module" <<'PYTHON'
import plistlib, sys
path, module = sys.argv[1], sys.argv[2]
with open(path, 'rb') as file:
    modules = plistlib.load(file)
if module not in modules:
    modules.append(module)
with open(path, 'wb') as file:
    plistlib.dump(modules, file)
print("enabled modules:", modules)
PYTHON
  pluginkit -e use -i "$module"
  killall fskit_agent 2>/dev/null
  sudo killall fskitd 2>/dev/null
  sleep 3
  pluginkit -m -v -p com.apple.fskit.fsmodule
  pgrep -fl fskit
}

try_mount() {
  store=$1
  mount_point=$2
  rm -rf "$store" "$mount_point"
  mkdir -p "$mount_point"
  volume=$("$built/stratafs-bench" create "$store") || { echo "create failed"; return 1; }
  echo "volume $volume"
  /sbin/mount -F -t stratafs -o "volume=$volume" "$store" "$mount_point"
  status=$?
  echo "mount exit $status"
  /sbin/mount | grep -i strata
  if /sbin/mount | grep -q "(stratafs"; then
    echo mounted > "$mount_point/file" && cat "$mount_point/file"
    ls -la "$mount_point"
    /sbin/umount "$mount_point"
    echo "umount exit $?"
    return 0
  fi
  return 1
}

diagnose() {
  log show --last 4m --style compact --predicate 'process == "fskitd" OR process == "fskit_agent" OR process == "amfid" OR process CONTAINS "stratafs" OR eventMessage CONTAINS[c] "stratafs" OR subsystem CONTAINS[c] "fskit"' 2>/dev/null | tail -80
}

/usr/bin/python3 - "$sources/extension.entitlements" "$RUNNER_TEMP/extension-without-module.entitlements" <<'PYTHON'
import plistlib, sys
with open(sys.argv[1], 'rb') as file:
    entitlements = plistlib.load(file)
entitlements.pop('com.apple.developer.fskit.fsmodule', None)
with open(sys.argv[2], 'wb') as file:
    plistlib.dump(entitlements, file)
PYTHON
assemble "$RUNNER_TEMP/extension-without-module.entitlements"
enable

echo "=== two mounts of one volume"
store=$RUNNER_TEMP/store-two
first=$RUNNER_TEMP/mount-first
second=$RUNNER_TEMP/mount-second
mkdir -p "$first" "$second"
volume=$("$built/stratafs-bench" create "$store")
echo "volume $volume"
/sbin/mount -F -t stratafs -o "volume=$volume" "$store" "$first"; echo "first mount exit $?"
/sbin/mount -F -t stratafs -o "volume=$volume" "$store" "$second"; echo "second mount exit $?"
/sbin/mount | grep -i strata
pgrep -fl stratafs-bench-extension
if [ "$(/sbin/mount | grep -c '(stratafs')" = 2 ]; then
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
  [ -e "$second/another" ] && echo "RESULT the second mount saw the new name" || echo "RESULT the second mount missed the new name"
else
  echo "RESULT two mounts of one volume did not both come up"
fi
/sbin/umount "$first"; /sbin/umount "$second"
log show --last 3m --style compact --predicate 'process CONTAINS "stratafs" OR (process == "fskitd" AND eventMessage CONTAINS[c] "cache") OR eventMessage CONTAINS[c] "setCacheState" OR eventMessage CONTAINS[c] "not permitted"' 2>/dev/null | tail -40
exit 0
