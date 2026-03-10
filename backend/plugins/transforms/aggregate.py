from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class Aggregate(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="aggregate",
            name="Aggregate",
            category=PluginCategory.TRANSFORM,
            description="Group by columns and aggregate with multiple functions",
            icon="BarChart3",
            color="#3b82f6",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="group_by", label="Group By (comma-separated)", param_type="string", required=True, description="Columns to group by"),
                ParamDefinition(name="aggregations", label="Aggregations (JSON)", param_type="code", required=True, description='[{"column":"amount","function":"sum","alias":"total"}]. Functions: sum, mean, min, max, count, first, last, std, var, median, n_unique, len'),
                ParamDefinition(name="maintain_order", label="Maintain Order", param_type="boolean", default=False, required=False, description="Keep original row order within groups (slower)"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        import json
        lf = inputs["input"]
        p = self.params
        group_cols = [c.strip() for c in p["group_by"].split(",") if c.strip()]
        aggs_raw = json.loads(p["aggregations"])

        agg_exprs = []
        for a in aggs_raw:
            col = pl.col(a["column"])
            func = a["function"]
            alias = a.get("alias", f"{a['column']}_{func}")
            expr = getattr(col, func)()
            agg_exprs.append(expr.alias(alias))

        return {"output": lf.group_by(group_cols, maintain_order=p.get("maintain_order", False)).agg(agg_exprs)}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        import json
        inv = input_vars.get("input", "df_in")
        p = params
        group_cols = [c.strip() for c in p["group_by"].split(",") if c.strip()]
        aggs_raw = json.loads(p["aggregations"])
        var = "__OUT__"

        agg_strs = []
        for a in aggs_raw:
            alias = a.get("alias", f"{a['column']}_{a['function']}")
            agg_strs.append(f"pl.col({a['column']!r}).{a['function']}().alias({alias!r})")

        mo = ", maintain_order=True" if p.get("maintain_order", False) else ""
        code = f"{var} = {inv}.group_by({group_cols!r}{mo}).agg([{', '.join(agg_strs)}])"
        return code, {"output": var}
