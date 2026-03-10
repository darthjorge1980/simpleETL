from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo, PortDefinition, PortType


@register
class JoinDataframes(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="join",
            name="Join",
            category=PluginCategory.TRANSFORM,
            description="Join two dataframes on key columns",
            icon="Merge",
            color="#3b82f6",
            inputs=[
                PortDefinition(name="left", port_type=PortType.DATAFRAME),
                PortDefinition(name="right", port_type=PortType.DATAFRAME),
            ],
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="left_on", label="Left Key(s) (comma-separated)", param_type="string", required=True, description="Column(s) from left DataFrame"),
                ParamDefinition(name="right_on", label="Right Key(s) (comma-separated)", param_type="string", required=True, description="Column(s) from right DataFrame"),
                ParamDefinition(name="how", label="Join Type", param_type="select", default="inner", options=["inner", "left", "right", "full", "semi", "anti", "cross"], required=False, description="Join strategy"),
                ParamDefinition(name="suffix", label="Suffix", param_type="string", default="_right", required=False, description="Suffix for duplicate column names from right"),
                ParamDefinition(name="validate", label="Validate", param_type="select", default="none", options=["none", "1:1", "1:m", "m:1", "m:m"], required=False, description="Validate join cardinality"),
                ParamDefinition(name="join_nulls", label="Join on Nulls", param_type="boolean", default=False, required=False, description="Treat null values as equal for joining"),
                ParamDefinition(name="coalesce", label="Coalesce Keys", param_type="boolean", default=True, required=False, description="Coalesce join keys into single columns"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        left = inputs["left"]
        right = inputs["right"]
        p = self.params

        left_on = [c.strip() for c in p["left_on"].split(",") if c.strip()]
        right_on = [c.strip() for c in p["right_on"].split(",") if c.strip()]

        kwargs: dict[str, Any] = dict(
            other=right,
            left_on=left_on if len(left_on) > 1 else left_on[0],
            right_on=right_on if len(right_on) > 1 else right_on[0],
            how=p.get("how", "inner"),
            suffix=p.get("suffix", "_right"),
            join_nulls=p.get("join_nulls", False),
            coalesce=p.get("coalesce", True),
        )

        validate = p.get("validate", "none")
        if validate and validate != "none":
            kwargs["validate"] = validate

        result = left.join(**kwargs)
        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        left_var = input_vars.get("left", "df_left")
        right_var = input_vars.get("right", "df_right")
        p = params
        var = "__OUT__"

        left_on = [c.strip() for c in p["left_on"].split(",") if c.strip()]
        right_on = [c.strip() for c in p["right_on"].split(",") if c.strip()]
        lo = left_on if len(left_on) > 1 else left_on[0]
        ro = right_on if len(right_on) > 1 else right_on[0]

        args = f"left_on={lo!r}, right_on={ro!r}, how={p.get('how', 'inner')!r}"

        suf = p.get("suffix", "_right")
        if suf != "_right":
            args += f", suffix={suf!r}"

        validate = p.get("validate", "none")
        if validate and validate != "none":
            args += f", validate={validate!r}"

        if p.get("join_nulls", False):
            args += ", join_nulls=True"
        if not p.get("coalesce", True):
            args += ", coalesce=False"

        code = f"{var} = {left_var}.join({right_var}, {args})"
        return code, {"output": var}
