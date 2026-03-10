from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo, PortDefinition, PortType


@register
class SwitchRouter(TransformPlugin):
    """Route rows to different outputs based on a condition."""

    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="switch_router",
            name="Switch / Router",
            category=PluginCategory.TRANSFORM,
            description="Route rows to matched or rejected output based on a condition",
            icon="Split",
            color="#d946ef",
            inputs=cls._base_inputs(),
            outputs=[
                PortDefinition(name="matched", port_type=PortType.DATAFRAME),
                PortDefinition(name="rejected", port_type=PortType.DATAFRAME),
            ],
            params=[
                ParamDefinition(name="condition", label="Condition", param_type="code", required=True, description="Polars boolean expression, e.g. pl.col('status') == 'active'"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params
        cond = eval(p["condition"], {"pl": pl})  # noqa: S307
        matched = lf.filter(cond)
        rejected = lf.filter(~cond)
        return {"matched": matched, "rejected": rejected}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        cond = p["condition"]
        code = f"df_matched = {iv}.filter({cond})\ndf_rejected = {iv}.filter(~({cond}))"
        return code, {"matched": "df_matched", "rejected": "df_rejected"}
