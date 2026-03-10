from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class DateOps(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="date_ops",
            name="Date Operations",
            category=PluginCategory.TRANSFORM,
            description="Extract or transform date/time components",
            icon="Calendar",
            color="#f97316",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="column", label="Column", param_type="string", required=True, description="Date/datetime column to transform"),
                ParamDefinition(name="operation", label="Operation", param_type="select", default="year", options=[
                    "year", "month", "day", "hour", "minute", "second",
                    "day_of_week", "day_of_year", "week", "quarter",
                    "strftime", "truncate", "offset_by",
                    "parse_str_to_date", "parse_str_to_datetime",
                    "epoch_seconds", "epoch_millis",
                ], required=True, description="Date operation to apply"),
                ParamDefinition(name="output_column", label="Output column", param_type="string", default="", required=False, description="Output column name (empty = overwrite)"),
                ParamDefinition(name="format", label="Format / every / offset", param_type="string", default="", required=False, description="strftime format, truncate interval (1d, 1mo), offset string (1d, -1w), or parse format"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params
        col = p["column"]
        op = p.get("operation", "year")
        out = p.get("output_column", "") or col
        fmt = p.get("format", "")

        expr = pl.col(col)
        if op == "year":
            expr = expr.dt.year()
        elif op == "month":
            expr = expr.dt.month()
        elif op == "day":
            expr = expr.dt.day()
        elif op == "hour":
            expr = expr.dt.hour()
        elif op == "minute":
            expr = expr.dt.minute()
        elif op == "second":
            expr = expr.dt.second()
        elif op == "day_of_week":
            expr = expr.dt.weekday()
        elif op == "day_of_year":
            expr = expr.dt.ordinal_day()
        elif op == "week":
            expr = expr.dt.week()
        elif op == "quarter":
            expr = expr.dt.quarter()
        elif op == "strftime":
            expr = expr.dt.strftime(fmt or "%Y-%m-%d")
        elif op == "truncate":
            expr = expr.dt.truncate(fmt or "1d")
        elif op == "offset_by":
            expr = expr.dt.offset_by(fmt or "1d")
        elif op == "parse_str_to_date":
            expr = expr.str.to_date(fmt or None)
        elif op == "parse_str_to_datetime":
            expr = expr.str.to_datetime(fmt or None)
        elif op == "epoch_seconds":
            expr = expr.dt.epoch("s")
        elif op == "epoch_millis":
            expr = expr.dt.epoch("ms")

        result = lf.with_columns(expr.alias(out))
        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        col = p["column"]
        op = p.get("operation", "year")
        out = p.get("output_column", "") or col
        code = f"{var} = {iv}.with_columns(pl.col({col!r}).dt.{op}().alias({out!r}))"
        return code, {"output": var}
