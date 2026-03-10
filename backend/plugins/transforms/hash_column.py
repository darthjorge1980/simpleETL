from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class HashColumn(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="hash_column",
            name="Checksum / Hash",
            category=PluginCategory.TRANSFORM,
            description="Generate a hash/checksum column from one or more columns",
            icon="Hash",
            color="#64748b",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="columns", label="Source column(s)", param_type="string", required=True, description="Columns to include in hash (comma-separated)"),
                ParamDefinition(name="output_column", label="Output column", param_type="string", default="hash_key", required=False, description="Name of the hash output column"),
                ParamDefinition(name="seed", label="Seed", param_type="number", default=0, required=False, description="Hash seed for reproducibility"),
                ParamDefinition(name="seed_1", label="Seed 1", param_type="number", default=0, required=False, description="Additional seed parameter"),
                ParamDefinition(name="seed_2", label="Seed 2", param_type="number", default=0, required=False, description="Additional seed parameter"),
                ParamDefinition(name="seed_3", label="Seed 3", param_type="number", default=0, required=False, description="Additional seed parameter"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params
        cols = [c.strip() for c in p["columns"].split(",") if c.strip()]
        out = p.get("output_column", "hash_key") or "hash_key"
        seed = int(p.get("seed", 0))
        seed_1 = int(p.get("seed_1", 0))
        seed_2 = int(p.get("seed_2", 0))
        seed_3 = int(p.get("seed_3", 0))

        concat_expr = pl.concat_str([pl.col(c).cast(pl.Utf8) for c in cols], separator="|")
        hash_expr = concat_expr.hash(seed=seed, seed_1=seed_1, seed_2=seed_2, seed_3=seed_3).alias(out)

        result = lf.with_columns(hash_expr)
        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        cols = [c.strip() for c in p["columns"].split(",") if c.strip()]
        out = p.get("output_column", "hash_key") or "hash_key"
        col_exprs = ", ".join(f"pl.col({c!r}).cast(pl.Utf8)" for c in cols)
        code = f'{var} = {iv}.with_columns(pl.concat_str([{col_exprs}], separator="|").hash().alias({out!r}))'
        return code, {"output": var}
