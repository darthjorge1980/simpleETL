from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import SourcePlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class CSVReader(SourcePlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="csv_reader",
            name="Read CSV",
            category=PluginCategory.SOURCE,
            description="Read data from a CSV file",
            icon="FileText",
            color="#22c55e",
            inputs=[],
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="file_path", label="File Path", param_type="string", required=True, description="Path to the CSV file (supports glob patterns)"),
                ParamDefinition(name="separator", label="Separator", param_type="string", default=",", required=False, description="Column separator character"),
                ParamDefinition(name="has_header", label="Has Header", param_type="boolean", default=True, required=False, description="First row contains column names"),
                ParamDefinition(name="encoding", label="Encoding", param_type="select", default="utf8", options=["utf8", "utf8-lossy"], required=False, description="utf8-lossy replaces invalid chars with \ufffd"),
                ParamDefinition(name="quote_char", label="Quote Character", param_type="string", default='"', required=False, description="Character used to quote fields (leave empty to disable)"),
                ParamDefinition(name="comment_prefix", label="Comment Prefix", param_type="string", default="", required=False, description="Lines starting with this are skipped"),
                ParamDefinition(name="eol_char", label="End-of-Line Character", param_type="select", default="\n", options=["\n", "\r\n"], required=False, description="Line terminator"),
                ParamDefinition(name="null_values", label="Null Values", param_type="string", default="", required=False, description="Comma-separated values to treat as null, e.g. NA,null,N/A"),
                ParamDefinition(name="skip_rows", label="Skip Rows (start)", param_type="number", default=0, required=False, description="Number of rows to skip at the beginning"),
                ParamDefinition(name="skip_rows_after_header", label="Skip Rows After Header", param_type="number", default=0, required=False, description="Rows to skip after the header row"),
                ParamDefinition(name="n_rows", label="Max Rows", param_type="number", default=0, required=False, description="Maximum number of rows to read (0 = all)"),
                ParamDefinition(name="infer_schema", label="Infer Schema", param_type="boolean", default=True, required=False, description="Automatically detect column types"),
                ParamDefinition(name="infer_schema_length", label="Infer Schema Length", param_type="number", default=100, required=False, description="Number of rows used to infer types"),
                ParamDefinition(name="try_parse_dates", label="Parse Dates", param_type="boolean", default=False, required=False, description="Try to automatically parse date columns"),
                ParamDefinition(name="ignore_errors", label="Ignore Errors", param_type="boolean", default=False, required=False, description="Skip rows that can't be parsed instead of failing"),
                ParamDefinition(name="truncate_ragged_lines", label="Truncate Ragged Lines", param_type="boolean", default=False, required=False, description="Truncate lines that are too long instead of erroring"),
                ParamDefinition(name="missing_utf8_is_empty_string", label="Missing UTF8 → Empty String", param_type="boolean", default=False, required=False, description="Treat missing UTF8 values as empty strings instead of null"),
                ParamDefinition(name="decimal_comma", label="Decimal Comma", param_type="boolean", default=False, required=False, description="Use comma as decimal separator (European format)"),
                ParamDefinition(name="low_memory", label="Low Memory Mode", param_type="boolean", default=False, required=False, description="Reduce memory usage at the cost of performance"),
                ParamDefinition(name="rechunk", label="Rechunk", param_type="boolean", default=False, required=False, description="Reallocate memory to contiguous chunks after reading"),
                ParamDefinition(name="raise_if_empty", label="Raise If Empty", param_type="boolean", default=True, required=False, description="Raise an error if the file is empty"),
                ParamDefinition(name="glob", label="Glob Patterns", param_type="boolean", default=True, required=False, description="Expand glob patterns in file path"),
                ParamDefinition(name="row_index_name", label="Row Index Column", param_type="string", default="", required=False, description="Add a row index column with this name (leave empty to skip)"),
                ParamDefinition(name="row_index_offset", label="Row Index Offset", param_type="number", default=0, required=False, description="Start value for the row index column"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        p = self.params
        null_values: str | list[str] | None = None
        nv_raw = p.get("null_values", "")
        if nv_raw:
            parts = [v.strip() for v in nv_raw.split(",") if v.strip()]
            null_values = parts if len(parts) > 1 else parts[0]

        kwargs: dict[str, Any] = dict(
            source=p["file_path"],
            separator=p.get("separator", ","),
            has_header=p.get("has_header", True),
            encoding=p.get("encoding", "utf8"),
            quote_char=p.get("quote_char", '"') or None,
            comment_prefix=p.get("comment_prefix", "") or None,
            eol_char=p.get("eol_char", "\n"),
            null_values=null_values,
            skip_rows=int(p.get("skip_rows", 0)),
            skip_rows_after_header=int(p.get("skip_rows_after_header", 0)),
            infer_schema=p.get("infer_schema", True),
            infer_schema_length=int(p.get("infer_schema_length", 100)),
            try_parse_dates=p.get("try_parse_dates", False),
            ignore_errors=p.get("ignore_errors", False),
            truncate_ragged_lines=p.get("truncate_ragged_lines", False),
            missing_utf8_is_empty_string=p.get("missing_utf8_is_empty_string", False),
            decimal_comma=p.get("decimal_comma", False),
            low_memory=p.get("low_memory", False),
            rechunk=p.get("rechunk", False),
            raise_if_empty=p.get("raise_if_empty", True),
            glob=p.get("glob", True),
        )

        n_rows = int(p.get("n_rows", 0))
        if n_rows > 0:
            kwargs["n_rows"] = n_rows

        row_idx = p.get("row_index_name", "")
        if row_idx:
            kwargs["row_index_name"] = row_idx
            kwargs["row_index_offset"] = int(p.get("row_index_offset", 0))

        lf = pl.scan_csv(**kwargs)
        return {"output": lf}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        p = params
        var = "__OUT__"
        args = [f"{p['file_path']!r}"]
        args.append(f"separator={p.get('separator', ',')!r}")
        args.append(f"has_header={p.get('has_header', True)!r}")
        args.append(f"encoding={p.get('encoding', 'utf8')!r}")

        qc = p.get("quote_char", '"')
        if qc:
            args.append(f"quote_char={qc!r}")
        else:
            args.append("quote_char=None")

        cp = p.get("comment_prefix", "")
        if cp:
            args.append(f"comment_prefix={cp!r}")

        eol = p.get("eol_char", "\n")
        if eol != "\n":
            args.append(f"eol_char={eol!r}")

        nv = p.get("null_values", "")
        if nv:
            parts = [v.strip() for v in nv.split(",") if v.strip()]
            args.append(f"null_values={parts!r}" if len(parts) > 1 else f"null_values={parts[0]!r}")

        sr = int(p.get("skip_rows", 0))
        if sr:
            args.append(f"skip_rows={sr}")

        srah = int(p.get("skip_rows_after_header", 0))
        if srah:
            args.append(f"skip_rows_after_header={srah}")

        nr = int(p.get("n_rows", 0))
        if nr > 0:
            args.append(f"n_rows={nr}")

        if not p.get("infer_schema", True):
            args.append("infer_schema=False")

        isl = int(p.get("infer_schema_length", 100))
        if isl != 100:
            args.append(f"infer_schema_length={isl}")

        if p.get("try_parse_dates", False):
            args.append("try_parse_dates=True")
        if p.get("ignore_errors", False):
            args.append("ignore_errors=True")
        if p.get("truncate_ragged_lines", False):
            args.append("truncate_ragged_lines=True")
        if p.get("missing_utf8_is_empty_string", False):
            args.append("missing_utf8_is_empty_string=True")
        if p.get("decimal_comma", False):
            args.append("decimal_comma=True")
        if p.get("low_memory", False):
            args.append("low_memory=True")
        if p.get("rechunk", False):
            args.append("rechunk=True")
        if not p.get("raise_if_empty", True):
            args.append("raise_if_empty=False")
        if not p.get("glob", True):
            args.append("glob=False")

        ri = p.get("row_index_name", "")
        if ri:
            args.append(f"row_index_name={ri!r}")
            rio = int(p.get("row_index_offset", 0))
            if rio:
                args.append(f"row_index_offset={rio}")

        code = f"{var} = pl.scan_csv(\n    {',\n    '.join(args)}\n)"
        return code, {"output": var}
