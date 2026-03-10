from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class WindowRank(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="window_rank",
            name="Window / Rank",
            category=PluginCategory.TRANSFORM,
            description="Apply window functions: rank, row_number, running totals, lag, lead",
            icon="Rows3",
            color="#a855f7",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="value_column", label="Value column", param_type="string", required=True, description="Column to apply the window function on"),
                ParamDefinition(name="function", label="Window function", param_type="select", default="row_number", options=[
                    "row_number", "rank", "dense_rank",
                    "cum_sum", "cum_max", "cum_min", "cum_count",
                    "lag", "lead",
                    "pct_change", "diff",
                    "rolling_mean", "rolling_sum", "rolling_min", "rolling_max",
                ], required=True, description="Window function to apply"),
                ParamDefinition(name="partition_by", label="Partition by", param_type="string", default="", required=False, description="Partition column(s) (comma-separated, empty = entire frame)"),
                ParamDefinition(name="order_by", label="Order by", param_type="string", default="", required=False, description="Sort column for ranking (comma-separated)"),
                ParamDefinition(name="descending", label="Descending", param_type="boolean", default=False, required=False, description="Sort in descending order"),
                ParamDefinition(name="output_column", label="Output column", param_type="string", default="", required=False, description="Output column name (empty = auto)"),
                ParamDefinition(name="n", label="N (lag/lead/rolling)", param_type="number", default=1, required=False, description="Offset for lag/lead, window size for rolling"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params
        val_col = p["value_column"]
        fn = p.get("function", "row_number")
        out = p.get("output_column", "") or f"{val_col}_{fn}"
        n = int(p.get("n", 1))

        partition_str = p.get("partition_by", "")
        partition = [c.strip() for c in partition_str.split(",") if c.strip()] if partition_str else None

        order_str = p.get("order_by", "")
        order_cols = [c.strip() for c in order_str.split(",") if c.strip()] if order_str else None
        desc = p.get("descending", False)

        if order_cols:
            lf = lf.sort(order_cols, descending=desc)

        expr = pl.col(val_col)

        if fn == "row_number":
            expr = pl.lit(1).cum_sum()
        elif fn == "rank":
            expr = expr.rank(method="min")
        elif fn == "dense_rank":
            expr = expr.rank(method="dense")
        elif fn == "cum_sum":
            expr = expr.cum_sum()
        elif fn == "cum_max":
            expr = expr.cum_max()
        elif fn == "cum_min":
            expr = expr.cum_min()
        elif fn == "cum_count":
            expr = expr.cum_count()
        elif fn == "lag":
            expr = expr.shift(n)
        elif fn == "lead":
            expr = expr.shift(-n)
        elif fn == "pct_change":
            expr = expr.pct_change(n)
        elif fn == "diff":
            expr = expr.diff(n)
        elif fn == "rolling_mean":
            expr = expr.rolling_mean(window_size=n)
        elif fn == "rolling_sum":
            expr = expr.rolling_sum(window_size=n)
        elif fn == "rolling_min":
            expr = expr.rolling_min(window_size=n)
        elif fn == "rolling_max":
            expr = expr.rolling_max(window_size=n)

        if partition:
            expr = expr.over(partition)

        result = lf.with_columns(expr.alias(out))
        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        val_col = p["value_column"]
        fn = p.get("function", "row_number")
        out = p.get("output_column", "") or f"{val_col}_{fn}"
        partition_str = p.get("partition_by", "")
        over = ""
        if partition_str:
            parts = [c.strip() for c in partition_str.split(",") if c.strip()]
            over = f".over({parts!r})"
        code = f"{var} = {iv}.with_columns(pl.col({val_col!r}).{fn}(){over}.alias({out!r}))"
        return code, {"output": var}
