#!/bin/bash

# Constants
REPOSITORY=$1
TAG=${2:-"latest"}
ARCH="$(uname -m | sed 's/amd64/x86_64/;s/arm64/aarch64/')"
OS="$(uname | tr '[:upper:]' '[:lower:]')"

case $TAG in 
    latest)
        API_URL="https://api.github.com/repos/$REPOSITORY/releases/$TAG"
        ;;
    *)
        API_URL="https://api.github.com/repos/$REPOSITORY/releases/tags/$TAG"
        ;;
esac

case $OS in
    darwin)
        OS_ARCH="$ARCH-apple-darwin"
        ;;
    linux)
        OS_ARCH="$ARCH-unknown-linux-gnu"
        ;;
    *)
        echo "unrecognized OS: $OS"
        exit 1
        ;;
esac

USER_PATH="$HOME"
BUNDLE_URL=$(curl -s $API_URL | python3 -c "import sys, json; print(next(iter([a.get('browser_download_url', '') for a in json.load(sys.stdin)['assets'] if '$OS_ARCH' in a['browser_download_url']]), ''))")

if [ -z "$BUNDLE_URL" ]; then
    echo -e "Failed to fetch release tag or download URL."
    exit 1
fi


# Setup dirs and download
DEST_DIR="$USER_PATH/Library/Application Support/app.agx/ch"
TMP_DIR=$(mktemp -d)
mkdir -p "$DEST_DIR/bin"
mkdir -p "$DEST_DIR/user_defined"
curl -L -s "$BUNDLE_URL" | tar xvf - -C "$TMP_DIR/" || echo "\u274c Failed to download bundle file"

# Move files to proper destinations
find "$TMP_DIR"/etc/clickhouse-server/ -name *.xml -exec cp {} "$DEST_DIR/user_defined/" \;
find "$TMP_DIR"/var/lib/clickhouse/user_defined/ -name *.sql -exec cp {} "$DEST_DIR/user_defined/" \;
find "$TMP_DIR"/var/lib/clickhouse/user_scripts/* -exec cp {} "$DEST_DIR/bin/" \;

# Set permissions and clean up
chmod +x "$DEST_DIR"/bin/*
rm -rf "$TMP_DIR"

echo "Module successfully extracted and set up in: $DEST_DIR"