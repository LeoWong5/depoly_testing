#!/usr/bin/env bash

if [ -z "${BASH_VERSION:-}" ]; then
	exec bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SITE_DIR="$(cd "${SCRIPT_DIR}/../site" && pwd)"
NEW_TESTING_DIR="${SCRIPT_DIR}/new"
NEW_WEBSERVER_DIR="${NEW_TESTING_DIR}/new_webserver"
DIST_DIR="${SITE_DIR}/dist"

printf "[1/4] Building front-end in %s\n" "${SITE_DIR}"
cd "${SITE_DIR}"
npm install
npm run build

printf "[2/4] Preparing new webserver output directory\n"
mkdir -p "${NEW_WEBSERVER_DIR}"
rm -rf "${NEW_WEBSERVER_DIR}/assets"

printf "[3/4] Copying built dist files to testing/new/new_webserver\n"
cp -f "${DIST_DIR}/index.html" "${NEW_WEBSERVER_DIR}/index.html"
cp -f "${DIST_DIR}/vite.svg" "${NEW_WEBSERVER_DIR}/vite.svg"
cp -R "${DIST_DIR}/assets" "${NEW_WEBSERVER_DIR}/assets"

printf "[4/4] Starting new webserver flow via Makefile (all_run.py)\n"
cd "${NEW_WEBSERVER_DIR}"
make run
