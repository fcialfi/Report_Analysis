#!/usr/bin/env python3
import argparse
import re
from pathlib import Path
from typing import Dict, List, Tuple

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


def _extract_start_time(path: Path) -> pd.Timestamp:
    excel = pd.ExcelFile(path)
    if "__meta" not in excel.sheet_names:
        available = ", ".join(excel.sheet_names)
        raise ValueError(
            f"Worksheet named '__meta' not found in {path}. "
            f"Available worksheets: {available or 'None'}."
        )
    meta = pd.read_excel(excel, sheet_name="__meta")
    if meta.empty:
        raise ValueError(f"Sheet '__meta' in {path} is empty.")
    start_time = None
    if "start_time_utc" in meta.columns:
        values = meta["start_time_utc"].dropna()
        if not values.empty:
            start_time = values.iloc[0]
    if start_time is None:
        lower_cols = [str(col).strip().lower() for col in meta.columns]
        for _, row in meta.iterrows():
            for idx, col in enumerate(meta.columns):
                cell = row[col]
                if isinstance(cell, str) and cell.strip().lower() == "start_time_utc":
                    next_value = None
                    if idx + 1 < len(meta.columns):
                        next_value = row[meta.columns[idx + 1]]
                    elif "value" in lower_cols:
                        value_idx = lower_cols.index("value")
                        next_value = row[meta.columns[value_idx]]
                    start_time = next_value
                    break
            if start_time is not None:
                break
    start_time = pd.to_datetime(start_time, errors="coerce", utc=True)
    if pd.isna(start_time):
        raise ValueError(f"Unable to parse start_time_utc from {path}.")
    return start_time


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
        frame = _process_file(path)
        start_time = _extract_start_time(path)
        processed_frames.append((frame, start_time))

    existing_sheets: Dict[str, pd.DataFrame] = {}
    if output_path.exists():
        existing_sheets = pd.read_excel(output_path, sheet_name=None, header=1)

    origin = min(start_time for _, start_time in processed_frames)
    nine_days = pd.Timedelta(days=9)
    grouped_frames: Dict[int, List[Tuple[pd.DataFrame, pd.Timestamp]]] = {}
    for frame, start_time in processed_frames:
        remainder = (start_time - origin) % nine_days
        key = int(remainder.total_seconds())
        grouped_frames.setdefault(key, []).append((frame, start_time))

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for idx, key in enumerate(sorted(grouped_frames)):
            sheet_name = f"Group {idx + 1}"
            group_items = sorted(grouped_frames[key], key=lambda item: item[1])
            output_frame = existing_sheets.get(sheet_name)
            if output_frame is not None:
                output_frame = output_frame.reset_index(drop=True)
            for frame, _ in group_items:
                orbit = int(frame["orbit"].iloc[0])
                columns = [
                    "time_iso_utc",
                    "6_1_azimuth",
                    "6_2_elevation",
                    "5_10_signal_noise_ratio",
                ]
                block = frame[columns].copy().reset_index(drop=True)
                block["time_iso_utc"] = block["time_iso_utc"].dt.tz_localize(None)
                block = block.rename(
                    columns={
                        "time_iso_utc": f"Orbit {orbit} time_iso_utc",
                        "6_1_azimuth": f"Orbit {orbit} 6_1_azimuth",
                        "6_2_elevation": f"Orbit {orbit} 6_2_elevation",
                        "5_10_signal_noise_ratio": (
                            f"Orbit {orbit} 5_10_signal_noise_ratio"
                        ),
                    }
                )
                if output_frame is None:
                    output_frame = block
                else:
                    output_frame = pd.concat(
                        [output_frame, block], axis=1, ignore_index=False
                    )
            if output_frame is None:
                output_frame = pd.DataFrame()
            output_frame.to_excel(
                writer,
                sheet_name=sheet_name,
                index=False,
                startrow=1,
            )
            worksheet = writer.sheets[sheet_name]
            if output_frame.shape[1] > 0:
                worksheet.merge_cells(
                    start_row=1,
                    start_column=1,
                    end_row=1,
                    end_column=output_frame.shape[1],
                )
                title_cell = worksheet.cell(row=1, column=1)
                title_cell.value = sheet_name
                title_cell.alignment = Alignment(horizontal="center")

    total_rows = sum(len(frame) for frame, _ in processed_frames)
    print(f"Saved {total_rows} rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
