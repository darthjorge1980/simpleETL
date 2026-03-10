from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo

_DTYPES = {
    "String": pl.Utf8,
    "Int32": pl.Int32,
    "Int64": pl.Int64,
    "Float32": pl.Float32,
    "Float64": pl.Float64,
    "Boolean": pl.Boolean,
    "Date": pl.Date,
    "Datetime": pl.Datetime,
    "UInt32": pl.UInt32,
    "UInt64": pl.UInt64,
}


@register
class CastType(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="cast_type",
            name="Cast Type",
            category=PluginCategory.TRANSFORM,
            description="Change the data type of one or more columns",
            icon="Binary",
            color="#ec4899",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="columns", label="Column(s)", param_type="string", required=True, description="Columns to cast (comma-separated)"),
                ParamDefinition(name="target_type", label="Target type", param_type="select", default="String", options=list(_DTYPES.keys()), required=True, description="Data type to cast to"),
                ParamDefinition(name="strict", label="Strict", param_type="boolean", default=False, required=False, description="If false, values that can't be cast become null instead of raising an error"),
                ParamDefinition(name="extra_casts", label="Extra casts (JSON)", param_type="code", default="[]", required=False, description='Additional casts: [{"columns":"col_a,col_b","target_type":"Int64"}]'),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        import json as _json
        lf = inputs["input"]
        p = self.params

        casts = [{"columns": p["columns"], "target_type": p["target_type"]}]
        extras = _json.loads(p.get("extra_casts", "[]") or "[]")
        casts.extend(extras)

        strict = p.get("strict", False)

        exprs = []
        for c in casts:
            cols = [x.strip() for x in c["columns"].split(",") if x.strip()]
            dtype = _DTYPES.get(c["target_type"], pl.Utf8)
            for col in cols:
                exprs.append(pl.col(col).cast(dtype, strict=strict))

        result = lf.with_columns(exprs)
        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        cols = [c.strip() for c in p["columns"].split(",") if c.strip()]
        dtype = p.get("target_type", "String")
        strict = p.get("strict", False)
        parts = [f"pl.col({c!r}).cast(pl.{dtype}, strict={strict})" for c in cols]
        code = f"{var} = {iv}.with_columns([{', '.join(parts)}])"
        return code, {"output": var}
