from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class FillNull(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="fill_null",
            name="Fill Null",
            category=PluginCategory.TRANSFORM,
            description="Replace null/missing values in columns",
            icon="Eraser",
            color="#ef4444",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="columns", label="Columns", param_type="string", default="", required=False, description="Columns to fill (comma-separated, empty = all)"),
                ParamDefinition(name="strategy", label="Strategy", param_type="select", default="value", options=["value", "forward", "backward", "mean", "min", "max", "zero", "one"], required=False, description="How to fill nulls"),
                ParamDefinition(name="fill_value", label="Fill value", param_type="string", default="", required=False, description="Literal value (used when strategy = value)"),
                ParamDefinition(name="limit", label="Forward/Backward limit", param_type="number", default=0, required=False, description="Max consecutive nulls to fill (0 = no limit, for forward/backward)"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params
        strategy = p.get("strategy", "value")

        cols_str = p.get("columns", "")
        cols = [c.strip() for c in cols_str.split(",") if c.strip()] if cols_str else None
        selector = [pl.col(c) for c in cols] if cols else [pl.all()]

        limit = int(p.get("limit", 0)) or None

        if strategy == "value":
            fill_val = p.get("fill_value", "")
            exprs = [c.fill_null(pl.lit(fill_val)) for c in selector]
        elif strategy == "forward":
            exprs = [c.forward_fill(limit=limit) for c in selector]
        elif strategy == "backward":
            exprs = [c.backward_fill(limit=limit) for c in selector]
        elif strategy == "mean":
            exprs = [c.fill_null(c.mean()) for c in selector]
        elif strategy == "min":
            exprs = [c.fill_null(c.min()) for c in selector]
        elif strategy == "max":
            exprs = [c.fill_null(c.max()) for c in selector]
        elif strategy == "zero":
            exprs = [c.fill_null(0) for c in selector]
        elif strategy == "one":
            exprs = [c.fill_null(1) for c in selector]
        else:
            exprs = [c.fill_null(pl.lit("")) for c in selector]

        result = lf.with_columns(exprs)
        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        strategy = p.get("strategy", "value")
        cols_str = p.get("columns", "")
        sel = f"pl.col({[c.strip() for c in cols_str.split(',') if c.strip()]!r})" if cols_str.strip() else "pl.all()"

        if strategy == "value":
            code = f"{var} = {iv}.with_columns({sel}.fill_null(pl.lit({p.get('fill_value', '')!r})))"
        elif strategy in ("forward", "backward"):
            fn = "forward_fill" if strategy == "forward" else "backward_fill"
            code = f"{var} = {iv}.with_columns({sel}.{fn}())"
        elif strategy in ("mean", "min", "max"):
            code = f"{var} = {iv}.with_columns({sel}.fill_null({sel}.{strategy}()))"
        else:
            val = 0 if strategy == "zero" else 1
            code = f"{var} = {iv}.with_columns({sel}.fill_null({val}))"
        return code, {"output": var}
