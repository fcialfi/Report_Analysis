#!/usr/bin/env python3
import argparse
from pathlib import Path

import pandas as pd


def _load_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    excel = pd.ExcelFile(path)
    if sheet_name not in excel.sheet_names:
        available = ", ".join(excel.sheet_names)
        raise ValueError(
            f"Worksheet named '{sheet_name}' not found in {path}. "
            f"Available worksheets: {available or 'None'}."
        )
    df = pd.read_excel(excel, sheet_name=sheet_name)
    if df.empty:
        raise ValueError(f"Sheet '{sheet_name}' in {path} is empty.")
    time_col = "time_iso_utc"
    if time_col not in df.columns:
        raise ValueError(
            f"Sheet '{sheet_name}' in {path} is missing required '{time_col}' column."
        )
    if sheet_name not in df.columns:
        raise ValueError(
            f"Sheet '{sheet_name}' in {path} is missing required '{sheet_name}' column."
        )
    data = df[[time_col, sheet_name]].copy()
    data[time_col] = pd.to_datetime(data[time_col], errors="coerce", utc=True)
    data = data.dropna(subset=[time_col]).copy()
    data[time_col] = data[time_col].dt.floor("s")
    data = data.sort_values(time_col)
    data = data.drop_duplicates(subset=[time_col], keep="first")
    return data


def _process_file(path: Path) -> pd.DataFrame:
    snr = _load_sheet(path, "5_10_signal_noise_ratio")
    azimuth = _load_sheet(path, "6_1_azimuth")
    elevation = _load_sheet(path, "6_2_elevation")

    merged = snr.merge(azimuth, on="time_iso_utc", how="inner").merge(
        elevation, on="time_iso_utc", how="inner"
    )
    merged = merged.sort_values("time_iso_utc")
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
    combined = combined.sort_values("time_iso_utc")
    combined = combined.drop_duplicates(subset=["time_iso_utc"], keep="first")

    output_path = Path(args.output).resolve()
    combined.to_excel(output_path, index=False)
    print(f"Saved {len(combined)} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
