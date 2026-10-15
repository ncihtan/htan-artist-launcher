#!/usr/bin/env python3
"""
check_samplesheet_uris.py

Read a CSV and verify that all S3 URIs in a given column exist.
Uses the AWS CLI/profile credentials named "tower".
Writes a report CSV with a boolean Exists column and an optional Error column.
Exits with code 0 if all exist, 1 otherwise.
"""

import argparse
import csv
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError


def parse_args():
    p = argparse.ArgumentParser(description="Check existence of S3 URIs in a CSV")
    p.add_argument("csv", help="Path to input CSV")
    p.add_argument(
        "--uri-column",
        help="Name of the column containing S3 URIs (if omitted, will auto-detect the first column whose values start with s3://)",
    )
    p.add_argument(
        "--output",
        default="s3_check_report.csv",
        help="Path to write results CSV (default: s3_check_report.csv)",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Number of parallel workers (default: 16)",
    )
    p.add_argument(
        "--treat-trailing-slash-as-prefix",
        action="store_true",
        help="If a key ends with '/', treat it as a prefix and check that at least one object exists under it.",
    )
    p.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Number of retries on transient errors (default: 2)",
    )
    return p.parse_args()


def get_s3(profile_name="tower"):
    # Use the requested profile
    try:
        session = boto3.Session(profile_name=profile_name)
        return session.client("s3")
    except (NoCredentialsError, Exception) as e:
        print(
            f"ERROR: Unable to initialize boto3 session with profile '{profile_name}': {e}",
            file=sys.stderr,
        )
        sys.exit(2)


def parse_s3_uri(uri: str):
    """
    Returns (bucket, key). Raises ValueError for invalid URIs.
    """
    if not uri or not uri.startswith("s3://"):
        raise ValueError("Not an s3:// URI")
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket:
        raise ValueError("Missing bucket")
    # key may be empty (object at bucket root is not valid, but prefixes can be empty).
    return bucket, key


def object_exists(s3, bucket: str, key: str, retries: int = 2):
    """
    Returns (exists: bool, error_message: str | None).
    - If key ends with '/' and --treat-trailing-slash-as-prefix is used, the caller will instead call prefix_exists.
    """
    attempt = 0
    while True:
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return True, None
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False, None
            # Retry on some transient-ish errors
            if (
                code in ("500", "503", "RequestTimeout", "SlowDown")
                and attempt < retries
            ):
                attempt += 1
                continue
            return False, f"ClientError {code}: {e}"
        except (EndpointConnectionError,) as e:
            if attempt < retries:
                attempt += 1
                continue
            return False, f"EndpointConnectionError: {e}"
        except Exception as e:
            return False, f"Unexpected error: {e}"


def prefix_exists(s3, bucket: str, prefix: str, retries: int = 2):
    """
    Check whether any object exists under the given prefix.
    """
    attempt = 0
    while True:
        try:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            return (resp.get("KeyCount", 0) > 0), None
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("NoSuchBucket", "AccessDenied"):
                return False, f"ClientError {code}: {e}"
            if (
                code in ("500", "503", "RequestTimeout", "SlowDown")
                and attempt < retries
            ):
                attempt += 1
                continue
            return False, f"ClientError {code}: {e}"
        except (EndpointConnectionError,) as e:
            if attempt < retries:
                attempt += 1
                continue
            return False, f"EndpointConnectionError: {e}"
        except Exception as e:
            return False, f"Unexpected error: {e}"


def main():
    args = parse_args()
    s3 = get_s3(profile_name="tower")

    # Load CSV
    with open(args.csv, newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        if not reader:
            print("No rows found in CSV.", file=sys.stderr)
            sys.exit(1)

        fieldnames = list(reader[0].keys())

    # Determine URI column
    uri_col = args.uri_column
    if not uri_col:
        # Auto-detect: first column where at least one value starts with s3://
        candidates = [
            c
            for c in fieldnames
            if any((row.get(c) or "").startswith("s3://") for row in reader)
        ]
        if not candidates:
            print(
                "Could not auto-detect a column with s3:// URIs. Use --uri-column.",
                file=sys.stderr,
            )
            sys.exit(1)
        uri_col = candidates[0]

    if uri_col not in fieldnames:
        print(
            f"Column '{uri_col}' not found in CSV header: {fieldnames}", file=sys.stderr
        )
        sys.exit(1)

    # Prepare tasks
    def check_one(row_idx, uri_value):
        uri_value = (uri_value or "").strip()
        if not uri_value:
            return row_idx, False, "Empty URI", uri_value
        try:
            bucket, key = parse_s3_uri(uri_value)
            if args.treat_trailing_slash_as_prefix and (key.endswith("/") or key == ""):
                exists, err = prefix_exists(s3, bucket, key, retries=args.retries)
            else:
                if key == "":
                    # If no key and not treating as prefix, it's ambiguous: treat as prefix for existence.
                    exists, err = prefix_exists(s3, bucket, key, retries=args.retries)
                    if err is None and not exists:
                        err = "Empty key; no objects found under bucket root"
                else:
                    exists, err = object_exists(s3, bucket, key, retries=args.retries)
            return row_idx, exists, err, uri_value
        except ValueError as e:
            return row_idx, False, f"Invalid URI: {e}", uri_value

    results = [None] * len(reader)
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(check_one, i, reader[i].get(uri_col)): i
            for i in range(len(reader))
        }
        for fut in as_completed(futures):
            row_idx, exists, err, uri_val = fut.result()
            results[row_idx] = (exists, err, uri_val)

    # Write output CSV: original columns + Exists + Error
    out_fields = fieldnames + ["Exists", "Error"]
    missing_count = 0
    with open(args.output, "w", newline="", encoding="utf-8") as out_f:
        w = csv.DictWriter(out_f, fieldnames=out_fields)
        w.writeheader()
        for i, row in enumerate(reader):
            exists, err, uri_val = results[i]
            row_out = dict(row)
            row_out["Exists"] = "TRUE" if exists else "FALSE"
            row_out["Error"] = "" if (err is None) else str(err)
            w.writerow(row_out)
            if not exists:
                missing_count += 1

    if missing_count > 0:
        print(f"Done. {missing_count} URI(s) missing or invalid. Report: {args.output}")
        sys.exit(1)
    else:
        print(f"Done. All URIs exist. Report: {args.output}")
        sys.exit(0)


if __name__ == "__main__":
    main()
