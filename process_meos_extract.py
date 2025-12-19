#!/usr/bin/env python3
import argparse
from pathlib import Path

import pandas as pd


def _normalize_time_column(df: pd.DataFrame) -> str:
    for col in df.columns:
        if "utc" in str(col).lower():
            return col
    return df.columns[0]


def _normalize_value_column(df: pd.DataFrame, time_col: str) -> str:
    candidates = [col for col in df.columns if col != time_col]
    if not candidates:
        return time_col
    return candidates[0]


def _load_sheet(path: Path, sheet_name: str, value_name: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet_name)
    if df.empty:
        raise ValueError(f"Sheet '{sheet_name}' in {path} is empty.")
    time_col = _normalize_time_column(df)
    value_col = _normalize_value_column(df, time_col)
    data = df[[time_col, value_col]].copy()
    data.columns = ["time_utc", value_name]
    data["time_utc"] = pd.to_datetime(data["time_utc"], errors="coerce", utc=True)
    data = data.dropna(subset=["time_utc"]).copy()
    data["time_utc"] = data["time_utc"].dt.floor("s")
    data = data.sort_values("time_utc")
    data = data.drop_duplicates(subset=["time_utc"], keep="first")
    return data


def _process_file(path: Path) -> pd.DataFrame:
    snr = _load_sheet(path, "signal_noise_ratio", "signal_noise_ratio")
    azimuth = _load_sheet(path, "azimuth", "azimuth")
    elevation = _load_sheet(path, "elevation", "elevation")

    merged = snr.merge(azimuth, on="time_utc", how="inner").merge(
        elevation, on="time_utc", how="inner"
    )
    merged = merged.sort_values("time_utc")
    return merged


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Process MEOS Extract Excel files and consolidate "
            "UTC time, SNR, azimuth, and elevation."
        )
    )
    parser.add_argument(
        "folder",
        nargs="?",
        default=".",
        help="Folder containing MEOS Extract Excel files.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="meos_processed.xlsx",
        help="Output Excel file path.",
    )
    args = parser.parse_args()

    folder = Path(args.folder).resolve()
    if not folder.exists() or not folder.is_dir():
        raise SystemExit(f"Folder not found: {folder}")

    excel_files = [
        *folder.glob("*.xlsx"),
        *folder.glob("*.xls"),
        *folder.glob("*.xlsm"),
    ]
    if not excel_files:
        raise SystemExit(f"No Excel files found in {folder}")

    combined_frames = []
    for path in sorted(excel_files):
        combined_frames.append(_process_file(path))

    combined = pd.concat(combined_frames, ignore_index=True)
    combined = combined.sort_values("time_utc")
    combined = combined.drop_duplicates(subset=["time_utc"], keep="first")

    output_path = Path(args.output).resolve()
    combined.to_excel(output_path, index=False)
    print(f"Saved {len(combined)} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
