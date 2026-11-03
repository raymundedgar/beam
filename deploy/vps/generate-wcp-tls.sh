#!/bin/sh
set -eu
host="${1:-localhost}"
dir="$(CDPATH= cd -- "$(dirname "$0")" && pwd)/data"
mkdir -p "$dir"
openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
  -keyout "$dir/wcp.key" -out "$dir/wcp.crt" \
  -subj "/CN=$host" \
  -addext "subjectAltName=DNS:$host,DNS:localhost,IP:127.0.0.1"
chmod 600 "$dir/wcp.key"
echo "wrote $dir/wcp.crt and $dir/wcp.key"
