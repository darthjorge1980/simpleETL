from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class Conditional(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="conditional",
            name="If / Then / Else",
            category=PluginCategory.TRANSFORM,
            description="Add a column based on conditional logic (CASE / WHEN)",
            icon="GitBranch",
            color="#eab308",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="output_column", label="Output column", param_type="string", required=True, description="Name for the result column"),
                ParamDefinition(name="condition", label="When condition", param_type="code", required=True, description="Polars expression that returns boolean, e.g. pl.col('age') >= 18"),
                ParamDefinition(name="then_value", label="Then value", param_type="code", required=True, description="Value or expression when true, e.g. pl.lit('adult') or pl.col('x') * 2"),
                ParamDefinition(name="otherwise_value", label="Otherwise value", param_type="code", default="pl.lit(None)", required=False, description="Value when false, e.g. pl.lit('minor')"),
                ParamDefinition(name="extra_whens", label="Extra WHEN clauses (JSON)", param_type="code", default="[]", required=False, description='Chain more conditions: [{"when":"pl.col(\'score\')>90","then":"pl.lit(\'A\')"}]'),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        import json as _json
        lf = inputs["input"]
        p = self.params

        cond = eval(p["condition"], {"pl": pl})  # noqa: S307
        then_val = eval(p["then_value"], {"pl": pl})  # noqa: S307
        other_val = eval(p.get("otherwise_value", "pl.lit(None)") or "pl.lit(None)", {"pl": pl})  # noqa: S307

        expr = pl.when(cond).then(then_val)

        extras = _json.loads(p.get("extra_whens", "[]") or "[]")
        for ew in extras:
            w = eval(ew["when"], {"pl": pl})  # noqa: S307
            t = eval(ew["then"], {"pl": pl})  # noqa: S307
            expr = expr.when(w).then(t)

        expr = expr.otherwise(other_val).alias(p["output_column"])
        result = lf.with_columns(expr)
        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        cond = p["condition"]
        then_val = p["then_value"]
        other_val = p.get("otherwise_value", "pl.lit(None)") or "pl.lit(None)"
        code = f"{var} = {iv}.with_columns(pl.when({cond}).then({then_val}).otherwise({other_val}).alias({p['output_column']!r}))"
        return code, {"output": var}
