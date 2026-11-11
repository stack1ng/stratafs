#!/bin/bash
# Assembles the benchmark FSKit module, enables it in FSKit's list of enabled modules, and mounts a volume with it.
# A hosted runner has no signing identity, so the module is signed ad hoc, without FSKit's module entitlement, which
# an ad hoc signature may not carry. Without it the module cannot drop what the kernel caches.
set -euo pipefail

workspace=$(pwd)
sources=$workspace/crates/stratafs-bench/bundle
built=$workspace/target/release
app=$workspace/target/bench/StrataBench.app
appex=$app/Contents/Extensions/StrataBench.appex
module=dev.stratafs.bench.extension
lsregister=/System/Library/Frameworks/CoreServices.framework/Versions/Current/Frameworks/LaunchServices.framework/Versions/Current/Support/lsregister
enabled="$HOME/Library/Group Containers/group.com.apple.fskit.settings/enabledModules.plist"

mkdir -p "$appex/Contents/MacOS" "$app/Contents/MacOS"
cp "$built/stratafs-bench-extension" "$appex/Contents/MacOS/"
cp "$sources/extension/Info.plist" "$appex/Contents/Info.plist"
cp "$built/stratafs-bench-host" "$app/Contents/MacOS/"
cp "$sources/host/Info.plist" "$app/Contents/Info.plist"
entitlements=$RUNNER_TEMP/extension.entitlements
cp "$sources/extension.entitlements" "$entitlements"
plutil -remove 'com\.apple\.developer\.fskit\.fsmodule' "$entitlements"
codesign --force --sign - --timestamp=none --options runtime --entitlements "$entitlements" --generate-entitlement-der "$appex"
codesign --force --sign - --timestamp=none --options runtime --entitlements "$sources/sandbox.entitlements" --generate-entitlement-der "$app"
"$lsregister" -f -R -trusted "$app"

# System Settings keeps the modules a user enabled in this list, which a runner can write, as its system protection is off.
/usr/libexec/PlistBuddy -c "Add :0 string $module" "$enabled"
pluginkit -e use -i "$module"
killall fskit_agent || true
sudo killall fskitd || true
sleep 3
pluginkit -m -p com.apple.fskit.fsmodule
pluginkit -m -p com.apple.fskit.fsmodule | grep -q "^+.*$module"

store=$RUNNER_TEMP/store
mount_point=$RUNNER_TEMP/mounted
mkdir -p "$mount_point"
volume=$("$built/stratafs-bench" create "$store")
/sbin/mount -F -t stratafs -o "volume=$volume" "$store" "$mount_point"
/sbin/mount | grep "(stratafs"
echo mounted > "$mount_point/file"
cat "$mount_point/file"
/sbin/umount "$mount_point"
! /sbin/mount | grep -q "(stratafs"
