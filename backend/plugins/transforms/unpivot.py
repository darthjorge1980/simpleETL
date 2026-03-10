from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class Unpivot(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="unpivot",
            name="Unpivot (Melt)",
            category=PluginCategory.TRANSFORM,
            description="Unpivot columns into rows (normalize / melt)",
            icon="FlipVertical",
            color="#f59e0b",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="on", label="Columns to unpivot", param_type="string", required=True, description="Columns to melt into rows (comma-separated)"),
                ParamDefinition(name="index", label="Index column(s)", param_type="string", default="", required=False, description="Columns to keep (comma-separated, empty = auto-detect)"),
                ParamDefinition(name="variable_name", label="Variable column name", param_type="string", default="variable", required=False, description="Name for the column holding original column names"),
                ParamDefinition(name="value_name", label="Value column name", param_type="string", default="value", required=False, description="Name for the column holding the values"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params

        on = [c.strip() for c in p["on"].split(",") if c.strip()]
        index_str = p.get("index", "")
        index = [c.strip() for c in index_str.split(",") if c.strip()] if index_str else None

        kwargs: dict[str, Any] = dict(
            on=on,
            variable_name=p.get("variable_name", "variable"),
            value_name=p.get("value_name", "value"),
        )
        if index:
            kwargs["index"] = index

        result = lf.unpivot(**kwargs)
        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        on = [c.strip() for c in p["on"].split(",") if c.strip()]
        args = f"on={on!r}, variable_name={p.get('variable_name', 'variable')!r}, value_name={p.get('value_name', 'value')!r}"
        index_str = p.get("index", "")
        if index_str:
            index = [c.strip() for c in index_str.split(",") if c.strip()]
            args += f", index={index!r}"
        code = f"{var} = {iv}.unpivot({args})"
        return code, {"output": var}
