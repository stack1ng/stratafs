#!/bin/bash
# Assembles the benchmark FSKit module, signs it with the Developer ID identity and provisioning profile the
# environment carries, enables it in FSKit's list of enabled modules, and mounts a volume with it.
set -euo pipefail

workspace=$(pwd)
sources=$workspace/crates/stratafs-bench/bundle
built=$workspace/target/release
app=$workspace/target/bench/StrataBench.app
appex=$app/Contents/Extensions/StrataBench.appex
module=dev.stratafs.bench.extension
lsregister=/System/Library/Frameworks/CoreServices.framework/Versions/Current/Frameworks/LaunchServices.framework/Versions/Current/Support/lsregister
enabled="$HOME/Library/Group Containers/group.com.apple.fskit.settings/enabledModules.plist"

# The identity goes into a keychain of its own, which codesign may use without asking.
keychain=$RUNNER_TEMP/signing.keychain-db
keychain_password=$(uuidgen)
security create-keychain -p "$keychain_password" "$keychain"
security set-keychain-settings "$keychain"
security unlock-keychain -p "$keychain_password" "$keychain"
base64 --decode <<< "$DEVELOPER_ID_CERTIFICATE" > "$RUNNER_TEMP/certificate.p12"
security import "$RUNNER_TEMP/certificate.p12" -k "$keychain" -P "$DEVELOPER_ID_CERTIFICATE_PASSWORD" -T /usr/bin/codesign > /dev/null
rm "$RUNNER_TEMP/certificate.p12"
security set-key-partition-list -S apple-tool:,apple: -s -k "$keychain_password" "$keychain" > /dev/null
security list-keychains -d user -s "$keychain" $(security list-keychains -d user | tr -d '"')
identity=$(security find-identity -v -p codesigning "$keychain" | awk '/Developer ID Application/ {print $2; exit}')

mkdir -p "$appex/Contents/MacOS" "$app/Contents/MacOS"
cp "$built/stratafs-bench-extension" "$appex/Contents/MacOS/"
cp "$sources/extension/Info.plist" "$appex/Contents/Info.plist"
cp "$built/stratafs-bench-host" "$app/Contents/MacOS/"
cp "$sources/host/Info.plist" "$app/Contents/Info.plist"
base64 --decode <<< "$FSKIT_PROVISIONING_PROFILE" > "$appex/Contents/embedded.provisionprofile"

# The module's signature carries the identifiers its profile grants, beside the entitlements the sources list.
profile=$RUNNER_TEMP/profile.plist
security cms -D -i "$appex/Contents/embedded.provisionprofile" > "$profile"
entitlements=$RUNNER_TEMP/extension.entitlements
cp "$sources/extension.entitlements" "$entitlements"
for granted in com.apple.application-identifier com.apple.developer.team-identifier; do
  value=$(/usr/libexec/PlistBuddy -c "Print :Entitlements:$granted" "$profile")
  /usr/libexec/PlistBuddy -c "Add :$granted string $value" "$entitlements"
done
codesign --force --sign "$identity" --keychain "$keychain" --timestamp=none --options runtime --entitlements "$entitlements" --generate-entitlement-der "$appex"
codesign --force --sign "$identity" --keychain "$keychain" --timestamp=none --options runtime --entitlements "$sources/sandbox.entitlements" --generate-entitlement-der "$app"
"$lsregister" -f -R -trusted "$app"

# System Settings keeps the modules a user enabled in this list, which a runner can write, as its system protection is off.
/usr/libexec/PlistBuddy -c "Add :0 string $module" "$enabled"
pluginkit -e use -i "$module"
killall fskit_agent || true
sudo killall fskitd || true
sleep 3
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
