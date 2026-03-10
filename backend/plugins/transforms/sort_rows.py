from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class SortRows(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="sort_rows",
            name="Sort",
            category=PluginCategory.TRANSFORM,
            description="Sort rows by one or more columns",
            icon="ArrowUpDown",
            color="#3b82f6",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="by", label="Sort By (comma-separated)", param_type="string", required=True, description="Column names to sort by"),
                ParamDefinition(name="descending", label="Descending", param_type="string", default="false", required=False, description="Comma-separated true/false per column, or a single value for all"),
                ParamDefinition(name="nulls_last", label="Nulls Last", param_type="string", default="false", required=False, description="Place nulls at end. Comma-separated true/false per column or single value"),
                ParamDefinition(name="maintain_order", label="Maintain Order", param_type="boolean", default=False, required=False, description="Stable sort preserving order of equal elements (slower)"),
                ParamDefinition(name="multithreaded", label="Multithreaded", param_type="boolean", default=True, required=False, description="Use multiple threads for sorting"),
            ],
        )

    @staticmethod
    def _parse_bool_list(val: str, length: int) -> list[bool] | bool:
        parts = [v.strip().lower() for v in val.split(",") if v.strip()]
        if len(parts) == 1:
            return parts[0] == "true"
        return [(p == "true") for p in parts][:length]

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params
        cols = [c.strip() for c in p["by"].split(",") if c.strip()]
        desc = self._parse_bool_list(str(p.get("descending", "false")), len(cols))
        nulls = self._parse_bool_list(str(p.get("nulls_last", "false")), len(cols))
        return {"output": lf.sort(
            cols,
            descending=desc,
            nulls_last=nulls,
            maintain_order=p.get("maintain_order", False),
            multithreaded=p.get("multithreaded", True),
        )}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        inv = input_vars.get("input", "df_in")
        p = params
        cols = [c.strip() for c in p["by"].split(",") if c.strip()]
        desc = SortRows._parse_bool_list(str(p.get("descending", "false")), len(cols))
        nulls = SortRows._parse_bool_list(str(p.get("nulls_last", "false")), len(cols))
        var = "__OUT__"
        args = f"{cols!r}, descending={desc!r}"
        if nulls != False:  # noqa: E712
            args += f", nulls_last={nulls!r}"
        if p.get("maintain_order", False):
            args += ", maintain_order=True"
        if not p.get("multithreaded", True):
            args += ", multithreaded=False"
        code = f"{var} = {inv}.sort({args})"
        return code, {"output": var}
