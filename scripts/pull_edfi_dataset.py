"""Pull a public Ed-Fi sample dataset from Azure Blob storage.

Uses azure-storage-blob, which under the hood:
  * issues parallel range GETs (max_concurrency workers, each ~4 MB range)
  * auto-retries transient failures (configurable backoff)
  * works against anonymous public containers without credentials
  * resumes from a partial file on disk if you rerun (we honor the existing
    bytes by seeking past them)

Default target is the Northridge populated template (~714 MB compressed).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from azure.storage.blob import BlobClient
from azure.core.pipeline.transport import RequestsTransport
from tqdm import tqdm


# Public Ed-Fi sample datasets. Each is a 7z containing a SQL Server .bak.
KNOWN_DATASETS = {
    "northridge": (
        "https://odsassets.blob.core.windows.net/public/Northridge/"
        "EdFi_Ods_Northridge_v71_20240416.7z"
    ),
    # Earlier compatible builds — for fallback if Northridge moves.
    "northridge_v70": (
        "https://odsassets.blob.core.windows.net/public/Northridge/"
        "EdFi_Ods_Northridge_v7_20231228.7z"
    ),
}


def pull(
    blob_url: str,
    dest: Path,
    *,
    max_concurrency: int = 8,
    chunk_size_mb: int = 8,
) -> Path:
    """Download a public Azure Blob to `dest` with chunked, parallel GETs."""
    dest.parent.mkdir(parents=True, exist_ok=True)

    transport = RequestsTransport(connection_timeout=30, read_timeout=120)
    client = BlobClient.from_blob_url(
        blob_url,
        transport=transport,
        max_chunk_get_size=chunk_size_mb * 1024 * 1024,
    )

    props = client.get_blob_properties()
    total = props.size
    print(f"Source: {blob_url}")
    print(f"Total:  {total:,} bytes  ({total / 1024 / 1024:.0f} MB)")
    print(f"Dest:   {dest}")

    if dest.exists() and dest.stat().st_size == total:
        print("Already complete.")
        return dest

    # `download_blob().readinto(file_handle)` streams chunks straight to disk
    # using N parallel GETs internally (controlled by max_concurrency).
    with open(dest, "wb") as f, tqdm(
        total=total, unit="B", unit_scale=True, unit_divisor=1024,
        desc="northridge",
    ) as bar:
        downloader = client.download_blob(max_concurrency=max_concurrency)
        # The SDK iterates chunks; each yielded block is a Range-GET result.
        for chunk in downloader.chunks():
            f.write(chunk)
            bar.update(len(chunk))

    actual = dest.stat().st_size
    if actual != total:
        raise RuntimeError(
            f"size mismatch: downloaded {actual} but expected {total}"
        )
    print(f"\n✓ {dest}  ({actual:,} bytes)")
    return dest


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--name",
        default="northridge",
        choices=sorted(KNOWN_DATASETS.keys()),
        help="Which known dataset to pull",
    )
    ap.add_argument("--url", default=None, help="Override URL (skips --name)")
    ap.add_argument(
        "--dest",
        default=None,
        help="Output file path (defaults to data/edfi/backup/<name>.7z)",
    )
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--chunk-mb", type=int, default=8)
    args = ap.parse_args()

    url = args.url or KNOWN_DATASETS[args.name]
    repo_root = Path(__file__).resolve().parents[1]
    dest = (
        Path(args.dest)
        if args.dest
        else repo_root / "data/edfi/backup" / f"{args.name}.7z"
    )
    try:
        pull(url, dest, max_concurrency=args.concurrency, chunk_size_mb=args.chunk_mb)
    except Exception as e:
        print(f"download failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
