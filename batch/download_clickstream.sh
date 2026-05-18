#!/usr/bin/env bash
# Download the English Wikipedia Clickstream dump for the configured month.
# Source: https://dumps.wikimedia.org/other/clickstream/
set -euo pipefail

MONTH="${CLICKSTREAM_MONTH:-2026-04}"
LANG="${CLICKSTREAM_LANG:-en}"
DEST="${CLICKSTREAM_PATH:-./data/clickstream}"

FILE="clickstream-${LANG}wiki-${MONTH}.tsv.gz"
URL="https://dumps.wikimedia.org/other/clickstream/${MONTH}/${FILE}"

mkdir -p "${DEST}"
cd "${DEST}"

if [[ -f "${FILE}" ]]; then
  echo "Already have ${FILE} — skipping."
  exit 0
fi

echo "Downloading ${URL}"
echo "(This is ~1.5 GB; expect 2-10 min depending on bandwidth.)"
curl -fL --retry 3 --retry-delay 5 -o "${FILE}.part" "${URL}"
mv "${FILE}.part" "${FILE}"

echo "Saved to $(pwd)/${FILE}"
ls -lh "${FILE}"
