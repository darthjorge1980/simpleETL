from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import DestinationPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class CSVWriter(DestinationPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="csv_writer",
            name="Write CSV",
            category=PluginCategory.DESTINATION,
            description="Write data to a CSV file",
            icon="FileDown",
            color="#f59e0b",
            inputs=cls._base_inputs(),
            outputs=[],
            params=[
                ParamDefinition(name="file_path", label="File Path", param_type="string", required=True, description="Output CSV file path"),
                ParamDefinition(name="separator", label="Separator", param_type="string", default=",", required=False, description="Column separator character"),
                ParamDefinition(name="include_header", label="Include Header", param_type="boolean", default=True, required=False, description="Write column names as the first row"),
                ParamDefinition(name="quote_char", label="Quote Character", param_type="string", default='"', required=False, description="Character used to quote fields"),
                ParamDefinition(name="quote_style", label="Quote Style", param_type="select", default="necessary", options=["necessary", "always", "non_numeric", "never"], required=False, description="When to quote fields"),
                ParamDefinition(name="line_terminator", label="Line Terminator", param_type="select", default="\n", options=["\n", "\r\n"], required=False, description="End-of-line character"),
                ParamDefinition(name="null_value", label="Null Representation", param_type="string", default="", required=False, description="String to write for null values (default: empty)"),
                ParamDefinition(name="include_bom", label="Include BOM", param_type="boolean", default=False, required=False, description="Include UTF-8 Byte Order Mark at the start"),
                ParamDefinition(name="datetime_format", label="Datetime Format", param_type="string", default="", required=False, description="Format string for datetime columns, e.g. %Y-%m-%d %H:%M:%S"),
                ParamDefinition(name="date_format", label="Date Format", param_type="string", default="", required=False, description="Format string for date columns, e.g. %Y-%m-%d"),
                ParamDefinition(name="time_format", label="Time Format", param_type="string", default="", required=False, description="Format string for time columns, e.g. %H:%M:%S"),
                ParamDefinition(name="float_precision", label="Float Precision", param_type="number", default=0, required=False, description="Number of decimal places for floats (0 = auto)"),
                ParamDefinition(name="float_scientific", label="Float Scientific Notation", param_type="select", default="auto", options=["auto", "always", "never"], required=False, description="When to use scientific notation for floats"),
                ParamDefinition(name="decimal_comma", label="Decimal Comma", param_type="boolean", default=False, required=False, description="Use comma as decimal separator (European format)"),
                ParamDefinition(name="batch_size", label="Batch Size", param_type="number", default=1024, required=False, description="Number of rows per write batch"),
                ParamDefinition(name="compression", label="Compression", param_type="select", default="uncompressed", options=["uncompressed", "gzip", "zstd"], required=False, description="Compress the output file"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        df = lf.collect(streaming=True)
        p = self.params

        kwargs: dict[str, Any] = dict(
            file=p["file_path"],
            separator=p.get("separator", ","),
            include_header=p.get("include_header", True),
            quote_char=p.get("quote_char", '"') or '"',
            line_terminator=p.get("line_terminator", "\n"),
            include_bom=p.get("include_bom", False),
            decimal_comma=p.get("decimal_comma", False),
            batch_size=int(p.get("batch_size", 1024)),
        )

        nv = p.get("null_value", "")
        if nv:
            kwargs["null_value"] = nv

        qs = p.get("quote_style", "necessary")
        if qs != "necessary":
            kwargs["quote_style"] = qs

        dtf = p.get("datetime_format", "")
        if dtf:
            kwargs["datetime_format"] = dtf
        df_fmt = p.get("date_format", "")
        if df_fmt:
            kwargs["date_format"] = df_fmt
        tf = p.get("time_format", "")
        if tf:
            kwargs["time_format"] = tf

        fp = int(p.get("float_precision", 0))
        if fp > 0:
            kwargs["float_precision"] = fp

        fs = p.get("float_scientific", "auto")
        if fs == "always":
            kwargs["float_scientific"] = True
        elif fs == "never":
            kwargs["float_scientific"] = False

        comp = p.get("compression", "uncompressed")
        if comp != "uncompressed":
            kwargs["compression"] = comp

        df.write_csv(**kwargs)
        return {}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        inv = input_vars.get("input", "df_in")
        p = params
        args = [f"{p['file_path']!r}"]
        args.append(f"separator={p.get('separator', ',')!r}")

        if not p.get("include_header", True):
            args.append("include_header=False")

        qc = p.get("quote_char", '"')
        if qc and qc != '"':
            args.append(f"quote_char={qc!r}")

        qs = p.get("quote_style", "necessary")
        if qs != "necessary":
            args.append(f"quote_style={qs!r}")

        lt = p.get("line_terminator", "\n")
        if lt != "\n":
            args.append(f"line_terminator={lt!r}")

        nv = p.get("null_value", "")
        if nv:
            args.append(f"null_value={nv!r}")

        if p.get("include_bom", False):
            args.append("include_bom=True")

        dtf = p.get("datetime_format", "")
        if dtf:
            args.append(f"datetime_format={dtf!r}")
        df_fmt = p.get("date_format", "")
        if df_fmt:
            args.append(f"date_format={df_fmt!r}")
        tf = p.get("time_format", "")
        if tf:
            args.append(f"time_format={tf!r}")

        fp = int(p.get("float_precision", 0))
        if fp > 0:
            args.append(f"float_precision={fp}")

        fs = p.get("float_scientific", "auto")
        if fs == "always":
            args.append("float_scientific=True")
        elif fs == "never":
            args.append("float_scientific=False")

        if p.get("decimal_comma", False):
            args.append("decimal_comma=True")

        bs = int(p.get("batch_size", 1024))
        if bs != 1024:
            args.append(f"batch_size={bs}")

        comp = p.get("compression", "uncompressed")
        if comp != "uncompressed":
            args.append(f"compression={comp!r}")

        code = f"{inv}.collect(streaming=True).write_csv(\n    {',\n    '.join(args)}\n)"
        return code, {}
