from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class DropDuplicates(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="drop_duplicates",
            name="Drop Duplicates",
            category=PluginCategory.TRANSFORM,
            description="Remove duplicate rows (Polars unique)",
            icon="Copy",
            color="#3b82f6",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="subset", label="Columns (comma-separated, blank = all)", param_type="string", default="", required=False, description="Columns to consider for deduplication"),
                ParamDefinition(name="keep", label="Keep", param_type="select", default="any", options=["any", "first", "last", "none"], required=False, description="Which duplicate to keep: any, first, last, or none (remove all)"),
                ParamDefinition(name="maintain_order", label="Maintain Order", param_type="boolean", default=False, required=False, description="Preserve original row order (slower)"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params
        subset_str = p.get("subset", "")
        subset = [c.strip() for c in subset_str.split(",") if c.strip()] or None
        return {"output": lf.unique(
            subset=subset,
            keep=p.get("keep", "any"),
            maintain_order=p.get("maintain_order", False),
        )}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        inv = input_vars.get("input", "df_in")
        p = params
        subset_str = p.get("subset", "")
        subset = [c.strip() for c in subset_str.split(",") if c.strip()] or None
        keep = p.get("keep", "any")
        mo = p.get("maintain_order", False)
        var = "__OUT__"
        args = f"subset={subset!r}"
        if keep != "any":
            args += f", keep={keep!r}"
        if mo:
            args += ", maintain_order=True"
        code = f"{var} = {inv}.unique({args})"
        return code, {"output": var}
