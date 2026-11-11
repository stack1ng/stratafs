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

echo "=== A: ad hoc with the module entitlement"
assemble "$sources/extension.entitlements"
enable
if try_mount "$RUNNER_TEMP/store-a" "$RUNNER_TEMP/mount-a"; then echo "RESULT A mounted"; else echo "RESULT A failed"; diagnose; fi

echo "=== B: ad hoc without the module entitlement"
grep -v -A1 "fskit.fsmodule" "$sources/extension.entitlements" > /dev/null
/usr/bin/python3 - "$sources/extension.entitlements" "$RUNNER_TEMP/extension-without-module.entitlements" <<'PYTHON'
import plistlib, sys
with open(sys.argv[1], 'rb') as file:
    entitlements = plistlib.load(file)
entitlements.pop('com.apple.developer.fskit.fsmodule', None)
with open(sys.argv[2], 'wb') as file:
    plistlib.dump(entitlements, file)
PYTHON
pkill -9 -f stratafs-bench-extension 2>/dev/null
assemble "$RUNNER_TEMP/extension-without-module.entitlements"
enable
if try_mount "$RUNNER_TEMP/store-b" "$RUNNER_TEMP/mount-b"; then echo "RESULT B mounted"; else echo "RESULT B failed"; diagnose; fi

echo "=== C: the store and mount point in the home directory"
if try_mount "$HOME/store-c" "$HOME/mount-c"; then echo "RESULT C mounted"; else echo "RESULT C failed"; diagnose; fi
exit 0
