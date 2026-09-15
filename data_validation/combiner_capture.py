# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Helper module to capture inputs and outputs of generate_report."""

import csv
import dataclasses
import datetime
import logging
import os
import shutil
import traceback
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

_CALL_COUNTER = 0


def _get_capture_dir() -> str:
    """Return the base directory for saving captured reports."""
    return os.environ.get("DVT_CAPTURE_DIR", "captured_reports")


def _get_next_call_index(base_dir: str) -> int:
    """Determine the next sequential call index based on existing folders."""
    global _CALL_COUNTER
    if os.path.exists(base_dir):
        try:
            existing = [
                d
                for d in os.listdir(base_dir)
                if os.path.isdir(os.path.join(base_dir, d)) and d.startswith("call_")
            ]
            highest = 0
            for d in existing:
                try:
                    num = int(d.split("_")[1])
                    if num > highest:
                        highest = num
                except (IndexError, ValueError):
                    pass
            _CALL_COUNTER = max(_CALL_COUNTER, highest)
        except Exception:
            pass
    _CALL_COUNTER += 1
    return _CALL_COUNTER


def _to_dataframe(table: Any) -> Optional[pd.DataFrame]:
    """Convert pyarrow.Table or pandas.DataFrame to pandas.DataFrame."""
    if table is None:
        return None
    if isinstance(table, pd.DataFrame):
        return table.copy()
    if hasattr(table, "to_pandas"):
        try:
            return table.to_pandas(
                timestamp_as_object=False, coerce_temporal_nanoseconds=True
            )
        except Exception:
            try:
                return table.to_pandas()
            except Exception:
                pass
    return None


def _sanitize_df_for_export(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Format binary or object columns safely so CSV serialization succeeds."""
    if df is None:
        return None
    try:
        export_df = df.copy()
        for col in export_df.columns:
            if export_df[col].dtype == object:
                export_df[col] = export_df[col].apply(
                    lambda x: x.hex() if isinstance(x, (bytes, bytearray)) else x
                )
        return export_df
    except Exception:
        return df


def _save_df_to_csv(df: Optional[pd.DataFrame], filepath: str) -> None:
    """Save DataFrame to CSV, handling binary columns and fallbacks safely."""
    if df is None:
        return
    try:
        safe_df = _sanitize_df_for_export(df)
        if safe_df is not None:
            safe_df.to_csv(filepath, index=False)
            return
    except Exception:
        pass

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(str(df))
    except Exception:
        pass


def _extract_validation_records(run_metadata: Any) -> List[Dict[str, Any]]:
    """Extract validation metadata list as dict records."""
    records = []
    if run_metadata is None:
        return records
    validations = getattr(run_metadata, "validations", {})
    if isinstance(validations, dict):
        for field_name, val in validations.items():
            rec: Dict[str, Any] = {"validation_name": field_name}
            if dataclasses.is_dataclass(val):
                rec.update(dataclasses.asdict(val))
            elif hasattr(val, "__dict__"):
                rec.update(vars(val))
            elif isinstance(val, dict):
                rec.update(val)
            else:
                rec["raw"] = str(val)
            records.append(rec)
    return records


def _save_validations_csv(records: List[Dict[str, Any]], filepath: str) -> None:
    """Save validation metadata records to a CSV file."""
    if not records:
        return
    try:
        fieldnames: List[str] = []
        for r in records:
            for k in r.keys():
                if k not in fieldnames:
                    fieldnames.append(k)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
    except Exception as e:
        logging.warning("Failed to save validations CSV: %s", e)


def capture_generate_report(
    run_metadata: Any,
    source_table: Any,
    target_table: Any,
    join_on_fields: Tuple = (),
    is_value_comparison: bool = False,
    verbose: bool = False,
    result_df: Optional[pd.DataFrame] = None,
    exception: Optional[Exception] = None,
) -> None:
    """Capture generate_report inputs and output to text and CSV files."""
    try:
        breakpoint()
        base_dir = _get_capture_dir()
        os.makedirs(base_dir, exist_ok=True)

        call_idx = _get_next_call_index(base_dir)
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        run_id = getattr(run_metadata, "run_id", "") or "unknown"
        clean_run_id = "".join(
            c if c.isalnum() or c in "-_" else "_" for c in str(run_id)
        )
        call_folder_name = f"call_{call_idx:03d}_{clean_run_id}".rstrip("_")
        call_dir = os.path.join(base_dir, call_folder_name)
        os.makedirs(call_dir, exist_ok=True)

        source_df = _to_dataframe(source_table)
        target_df = _to_dataframe(target_table)
        out_df = _to_dataframe(result_df)

        source_rows = len(source_df) if source_df is not None else 0
        source_cols = len(source_df.columns) if source_df is not None else 0
        target_rows = len(target_df) if target_df is not None else 0
        target_cols = len(target_df.columns) if target_df is not None else 0
        out_rows = len(out_df) if out_df is not None else 0
        out_cols = len(out_df.columns) if out_df is not None else 0

        status = (
            "SUCCESS"
            if exception is None
            else f"FAILED: {type(exception).__name__}: {exception}"
        )

        # 1. Save CSV files in call_dir
        source_csv = os.path.join(call_dir, "input_source_table.csv")
        target_csv = os.path.join(call_dir, "input_target_table.csv")
        _save_df_to_csv(source_df, source_csv)
        _save_df_to_csv(target_df, target_csv)

        validations = _extract_validation_records(run_metadata)
        if validations:
            val_csv = os.path.join(call_dir, "input_validations.csv")
            _save_validations_csv(validations, val_csv)

        if out_df is not None:
            out_csv = os.path.join(call_dir, "output_report.csv")
            _save_df_to_csv(out_df, out_csv)

        # 2. Save input_parameters.txt in call_dir
        params_txt = os.path.join(call_dir, "input_parameters.txt")
        with open(params_txt, "w", encoding="utf-8") as f:
            f.write(f"Call Index: {call_idx}\n")
            f.write(f"Timestamp: {timestamp}\n")
            f.write(f"Status: {status}\n")
            f.write(f"Run ID: {run_id}\n")
            f.write(f"join_on_fields: {tuple(join_on_fields)}\n")
            f.write(f"is_value_comparison: {is_value_comparison}\n")
            f.write(f"verbose: {verbose}\n\n")

            f.write("Run Metadata:\n")
            if run_metadata is not None:
                f.write(f"  run_id: {getattr(run_metadata, 'run_id', None)}\n")
                f.write(f"  start_time: {getattr(run_metadata, 'start_time', None)}\n")
                f.write(f"  end_time: {getattr(run_metadata, 'end_time', None)}\n")
                f.write(f"  labels: {getattr(run_metadata, 'labels', None)}\n")
                f.write(f"  validations_count: {len(validations)}\n")
            else:
                f.write("  None\n")

            f.write("\nSource Table:\n")
            f.write(f"  Type: {type(source_table).__name__}\n")
            f.write(f"  Shape: ({source_rows}, {source_cols})\n")
            if source_df is not None:
                f.write(f"  Columns: {list(source_df.columns)}\n")
                f.write(f"  Dtypes:\n{source_df.dtypes.to_string()}\n")

            f.write("\nTarget Table:\n")
            f.write(f"  Type: {type(target_table).__name__}\n")
            f.write(f"  Shape: ({target_rows}, {target_cols})\n")
            if target_df is not None:
                f.write(f"  Columns: {list(target_df.columns)}\n")
                f.write(f"  Dtypes:\n{target_df.dtypes.to_string()}\n")

        # 3. Save output text or error in call_dir
        if out_df is not None:
            out_txt = os.path.join(call_dir, "output_report.txt")
            with open(out_txt, "w", encoding="utf-8") as f:
                f.write(f"Output Report Shape: ({out_rows}, {out_cols})\n")
                f.write(f"Columns: {list(out_df.columns)}\n\n")
                safe_out = _sanitize_df_for_export(out_df)
                if safe_out is not None:
                    f.write(safe_out.to_string(index=False, max_rows=100))
        if exception is not None:
            err_txt = os.path.join(call_dir, "error.txt")
            with open(err_txt, "w", encoding="utf-8") as f:
                f.write(f"Exception Type: {type(exception).__name__}\n")
                f.write(f"Exception Message: {exception}\n\n")
                f.write("Traceback:\n")
                f.write(
                    "".join(
                        traceback.format_exception(
                            type(exception), exception, exception.__traceback__
                        )
                    )
                )

        # 4. Update 'latest' copies in base_dir
        try:
            latest_source = os.path.join(base_dir, "latest_source_table.csv")
            latest_target = os.path.join(base_dir, "latest_target_table.csv")
            latest_params = os.path.join(base_dir, "latest_parameters.txt")
            _save_df_to_csv(source_df, latest_source)
            _save_df_to_csv(target_df, latest_target)
            shutil.copyfile(params_txt, latest_params)

            if validations:
                latest_val = os.path.join(base_dir, "latest_validations.csv")
                _save_validations_csv(validations, latest_val)

            if out_df is not None:
                latest_out = os.path.join(base_dir, "latest_output_report.csv")
                _save_df_to_csv(out_df, latest_out)
        except Exception:
            pass

        # 5. Append to summary.csv in base_dir
        summary_csv = os.path.join(base_dir, "summary.csv")
        file_exists = os.path.exists(summary_csv)
        try:
            with open(summary_csv, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(
                        [
                            "call_index",
                            "timestamp",
                            "run_id",
                            "status",
                            "join_on_fields",
                            "is_value_comparison",
                            "source_rows",
                            "source_cols",
                            "target_rows",
                            "target_cols",
                            "output_rows",
                            "output_cols",
                            "folder",
                        ]
                    )
                writer.writerow(
                    [
                        call_idx,
                        timestamp,
                        run_id,
                        "SUCCESS" if exception is None else type(exception).__name__,
                        str(tuple(join_on_fields)),
                        is_value_comparison,
                        source_rows,
                        source_cols,
                        target_rows,
                        target_cols,
                        out_rows,
                        out_cols,
                        call_folder_name,
                    ]
                )
        except Exception:
            pass

        # 6. Append to generate_report_log.txt in base_dir
        log_file = os.path.join(base_dir, "generate_report_log.txt")
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write("=" * 80 + "\n")
                f.write(
                    f"CALL #{call_idx} | {timestamp} | Run ID: {run_id} | Status: {status}\n"
                )
                f.write(f"Folder: {call_dir}\n")
                f.write("-" * 80 + "\n")
                f.write("INPUT PARAMETERS:\n")
                f.write(f"  join_on_fields: {tuple(join_on_fields)}\n")
                f.write(f"  is_value_comparison: {is_value_comparison}\n")
                f.write(f"  verbose: {verbose}\n")
                f.write("  run_metadata:\n")
                if run_metadata is not None:
                    f.write(f"    run_id: {getattr(run_metadata, 'run_id', None)}\n")
                    f.write(
                        f"    start_time: {getattr(run_metadata, 'start_time', None)}\n"
                    )
                    f.write(
                        f"    end_time: {getattr(run_metadata, 'end_time', None)}\n"
                    )
                    f.write(f"    validations: {len(validations)} validation(s)\n")
                    for v in validations:
                        f.write(
                            f"      - {v.get('validation_name')}: "
                            f"type={v.get('validation_type')}, agg={v.get('aggregation_type')}, "
                            f"src={v.get('source_table_name')}.{v.get('source_column_name')}, "
                            f"tgt={v.get('target_table_name')}.{v.get('target_column_name')}\n"
                        )
                else:
                    f.write("    None\n")

                f.write(f"\nSource Table: ({source_rows} rows, {source_cols} cols)\n")
                if source_df is not None:
                    safe_src = _sanitize_df_for_export(source_df)
                    if safe_src is not None:
                        f.write(safe_src.head(10).to_string(index=False) + "\n")

                f.write(f"\nTarget Table: ({target_rows} rows, {target_cols} cols)\n")
                if target_df is not None:
                    safe_tgt = _sanitize_df_for_export(target_df)
                    if safe_tgt is not None:
                        f.write(safe_tgt.head(10).to_string(index=False) + "\n")

                f.write("-" * 80 + "\n")
                f.write("OUTPUT:\n")
                if out_df is not None:
                    f.write(f"Result DataFrame: ({out_rows} rows, {out_cols} cols)\n")
                    safe_out = _sanitize_df_for_export(out_df)
                    if safe_out is not None:
                        f.write(safe_out.to_string(index=False, max_rows=50) + "\n")
                elif exception is not None:
                    f.write(f"ERROR: {type(exception).__name__}: {exception}\n")

                f.write("=" * 80 + "\n\n")
        except Exception:
            pass

        logging.info("Captured generate_report call #%d to %s", call_idx, call_dir)
    except Exception as e:
        logging.warning("combiner_capture failed: %s", e)
