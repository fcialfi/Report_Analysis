#!/usr/bin/env python3
import argparse
import re
from pathlib import Path

import pandas as pd
from openpyxl.styles import Alignment


def _load_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    excel = pd.ExcelFile(path)
    resolved_sheet = sheet_name
    if sheet_name not in excel.sheet_names:
        matching_sheets = []
        for candidate in excel.sheet_names:
            preview = pd.read_excel(excel, sheet_name=candidate, nrows=0)
            if sheet_name in preview.columns:
                matching_sheets.append(candidate)
        if len(matching_sheets) == 1:
            resolved_sheet = matching_sheets[0]
        else:
            available = ", ".join(excel.sheet_names)
            if matching_sheets:
                matches = ", ".join(matching_sheets)
                raise ValueError(
                    f"Worksheet named '{sheet_name}' not found in {path}. "
                    f"Multiple worksheets contain '{sheet_name}': {matches}."
                )
            raise ValueError(
                f"Worksheet named '{sheet_name}' not found in {path}. "
                f"Available worksheets: {available or 'None'}."
            )
    time_col = "time_iso_utc"
    df = pd.read_excel(excel, sheet_name=resolved_sheet)
    if df.empty:
        print(f"Warning: sheet '{sheet_name}' in {path} is empty; skipping data.")
        empty_frame = pd.DataFrame(columns=[time_col, sheet_name])
        return empty_frame
    if time_col not in df.columns:
        raise ValueError(
            f"Sheet '{sheet_name}' in {path} is missing required '{time_col}' column."
        )
    if sheet_name not in df.columns:
        raise ValueError(
            f"Sheet '{resolved_sheet}' in {path} is missing required '{sheet_name}' column."
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
    merged["orbit"] = _extract_orbit(path)
    merged = merged.sort_values("time_iso_utc")
    return merged


def _extract_orbit(path: Path) -> int:
    match = re.search(r"orbit[_-]?(\d+)", path.stem, flags=re.IGNORECASE)
    if not match:
        raise ValueError(
            f"Unable to determine orbit from filename '{path.name}'. "
            "Expected pattern like 'orbit_2648'."
        )
    return int(match.group(1))


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

    output_path = Path(args.output).resolve()
    excel_files = [
        *folder.glob("*.xlsx"),
        *folder.glob("*.xls"),
        *folder.glob("*.xlsm"),
    ]
    excel_files = [path for path in excel_files if path.resolve() != output_path]
    if not excel_files:
        raise SystemExit(f"No Excel files found in {folder}")

    processed_frames = []
    for path in sorted(excel_files):
        processed_frames.append(_process_file(path))

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for frame in processed_frames:
            orbit = int(frame["orbit"].iloc[0])
            sheet_name = f"Orbit {orbit}"
            columns = ["5_10_signal_noise_ratio", "6_1_azimuth", "6_2_elevation"]
            output_frame = frame[columns].copy()
            output_frame.to_excel(
                writer,
                sheet_name=sheet_name,
                index=False,
                startrow=1,
            )
            worksheet = writer.sheets[sheet_name]
            worksheet.merge_cells(start_row=1, start_column=1, end_row=1, end_column=3)
            title_cell = worksheet.cell(row=1, column=1)
            title_cell.value = sheet_name
            title_cell.alignment = Alignment(horizontal="center")

    total_rows = sum(len(frame) for frame in processed_frames)
    print(f"Saved {total_rows} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
