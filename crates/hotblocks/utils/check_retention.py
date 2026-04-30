#!/usr/bin/env python3
"""
Tool to analyze retention strategy vs actual data range for datasets.

Metrics computed per dataset:
  - pre_retention  = retentionStrategy.FromBlock.number - data.firstBlock
                     (how many blocks before the retention cutoff we still have)
  - retention_len  = data.lastBlock - retentionStrategy.FromBlock.number
                     (how many blocks are inside the retention window)
  - pre_pct        = pre_retention / retention_len * 100  (pre-retention as % of retention length)

Dataset IDs are auto-discovered from /metrics (Prometheus format) by extracting
all unique dataset="..." labels. A static --datasets file can be used as fallback
or override.
"""

import argparse
import json
import re
import sys
import urllib.request
import urllib.error
from pathlib import Path


def fetch_datasets_from_metrics(host: str, port: int) -> list[str]:
    """Parse unique dataset IDs from the Prometheus /metrics endpoint."""
    url = f"http://{host}:{port}/metrics"
    with urllib.request.urlopen(url, timeout=10) as resp:
        body = resp.read().decode()
    ids = sorted(set(re.findall(r'dataset="([^"]+)"', body)))
    return ids


def fetch_status(host: str, port: int, dataset: str) -> dict:
    url = f"http://{host}:{port}/datasets/{dataset}/status"
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read())


def analyze(data: dict, dataset: str) -> dict | None:
    retention = data.get("retentionStrategy", {})
    from_block = retention.get("FromBlock")
    if from_block is None:
        return None  # no retention strategy

    retention_number = from_block.get("number")
    data_section = data.get("data", {})
    first_block = data_section.get("firstBlock")
    last_block = data_section.get("lastBlock")

    if retention_number is None or first_block is None or last_block is None:
        return None

    pre_retention = retention_number - first_block
    retention_len = last_block - retention_number

    if retention_len == 0:
        pre_pct = float("inf")
    else:
        pre_pct = pre_retention / retention_len * 100

    return {
        "dataset": dataset,
        "first_block": first_block,
        "retention_from": retention_number,
        "last_block": last_block,
        "pre_retention": pre_retention,
        "retention_len": retention_len,
        "pre_pct": pre_pct,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Check retention strategy vs actual data range for datasets."
    )
    parser.add_argument(
        "--host", default="127.0.0.1", help="API host (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", type=int, default=8081, help="API port (default: 8081)"
    )
    parser.add_argument(
        "--datasets",
        default=None,
        help="Path to file with dataset names, one per line. "
        "If omitted, dataset IDs are auto-discovered from /metrics.",
    )
    args = parser.parse_args()

    if args.datasets is not None:
        datasets_path = Path(args.datasets)
        if not datasets_path.exists():
            print(f"ERROR: datasets file not found: {datasets_path}", file=sys.stderr)
            sys.exit(1)
        datasets = [
            line.strip()
            for line in datasets_path.read_text().splitlines()
            if line.strip()
        ]
        print(
            f"Loaded {len(datasets)} dataset(s) from {datasets_path}", file=sys.stderr
        )
    else:
        try:
            datasets = fetch_datasets_from_metrics(args.host, args.port)
            print(
                f"Discovered {len(datasets)} dataset(s) from "
                f"http://{args.host}:{args.port}/metrics",
                file=sys.stderr,
            )
        except Exception as e:
            print(f"ERROR: failed to fetch /metrics: {e}", file=sys.stderr)
            sys.exit(1)

    results = []
    errors = []

    for dataset in datasets:
        try:
            raw = fetch_status(args.host, args.port, dataset)
            result = analyze(raw, dataset)
            if result is None:
                errors.append(
                    (
                        dataset,
                        "missing required fields or no FromBlock retention strategy",
                    )
                )
            else:
                results.append(result)
        except urllib.error.HTTPError as e:
            errors.append((dataset, f"HTTP {e.code}: {e.reason}"))
        except Exception as e:
            errors.append((dataset, str(e)))

    # Print table header
    col_w = {
        "dataset": max(
            len("dataset"), max((len(r["dataset"]) for r in results), default=0)
        ),
        "first_block": 12,
        "retention_from": 15,
        "last_block": 12,
        "pre_retention": 14,
        "retention_len": 14,
        "pre_pct": 10,
    }

    header = (
        f"{'dataset':<{col_w['dataset']}}  "
        f"{'first_block':>{col_w['first_block']}}  "
        f"{'retention_from':>{col_w['retention_from']}}  "
        f"{'last_block':>{col_w['last_block']}}  "
        f"{'pre_retention':>{col_w['pre_retention']}}  "
        f"{'retention_len':>{col_w['retention_len']}}  "
        f"{'pre_pct %':>{col_w['pre_pct']}}"
    )
    separator = "-" * len(header)

    print(separator)
    print(header)
    print(separator)

    for r in results:
        pre_pct_str = f"{r['pre_pct']:.2f}" if r["pre_pct"] != float("inf") else "inf"
        print(
            f"{r['dataset']:<{col_w['dataset']}}  "
            f"{r['first_block']:>{col_w['first_block']},}  "
            f"{r['retention_from']:>{col_w['retention_from']},}  "
            f"{r['last_block']:>{col_w['last_block']},}  "
            f"{r['pre_retention']:>{col_w['pre_retention']},}  "
            f"{r['retention_len']:>{col_w['retention_len']},}  "
            f"{pre_pct_str:>{col_w['pre_pct']}}"
        )

    print(separator)

    if results:
        total_pre = sum(r["pre_retention"] for r in results)
        total_ret = sum(r["retention_len"] for r in results)
        total_all = total_pre + total_ret
        total_pct = total_pre / total_ret * 100 if total_ret else float("inf")
        total_pct_str = f"{total_pct:.2f}" if total_pct != float("inf") else "inf"

        label = "TOTAL"
        print(
            f"{label:<{col_w['dataset']}}  "
            f"{'':>{col_w['first_block']}}  "
            f"{'':>{col_w['retention_from']}}  "
            f"{'':>{col_w['last_block']}}  "
            f"{total_pre:>{col_w['pre_retention']},}  "
            f"{total_ret:>{col_w['retention_len']},}  "
            f"{total_pct_str:>{col_w['pre_pct']}}"
        )

        grand_label = "GRAND TOTAL"
        grand_str = f"{total_all:,}"
        print(f"{grand_label:<{col_w['dataset']}}  {grand_str}")

        print(separator)

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for dataset, msg in errors:
            print(f"  {dataset}: {msg}")


if __name__ == "__main__":
    main()
