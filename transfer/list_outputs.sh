SRC_BUCKET=htan-project-tower-bucket
PREFIX=outputs/artist_oxqp/   # include trailing slash if that’s how it’s stored
MANIFEST=artist_oxqp_output_ls.csv

# CSV lines: Bucket,Key (no header)
aws s3api --profile htan-dev list-objects-v2 --bucket "$SRC_BUCKET" --prefix "$PREFIX" --output json --page-size 1000 \
  | jq -r '.Contents[]? | @csv' \
  | sed -E "s/^\"([^\"]+)\",\"([^\"]+)\".*$/\"$SRC_BUCKET\",\"\\2\"/" > "$MANIFEST"
#!/usr/bin/env bash
set -euo pipefail

# Source bucket and prefix to enumerate
SRC_BUCKET=htan-project-tower-bucket
PREFIX=outputs/artist_oxqp/   # include trailing slash if that’s how it’s stored

# Output manifest path (CSV with no header): Bucket,Key
MANIFEST=artist_oxqp_output_ls.csv

# Generate CSV manifest directly with jq (no sed needed)
# This builds an array [Bucket, Key] per object so @csv can format it.
aws s3api --profile htan-dev list-objects-v2 \
  --bucket "$SRC_BUCKET" \
  --prefix "$PREFIX" \
  --output json \
  --page-size 1000 \
  | jq -r --arg b "$SRC_BUCKET" '.Contents[]? | [$b, .Key] | @csv' \
  > "$MANIFEST"

echo "Wrote $(wc -l < "$MANIFEST") lines to $MANIFEST"