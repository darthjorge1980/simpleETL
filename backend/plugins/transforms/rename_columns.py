from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class RenameColumns(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="rename_columns",
            name="Rename Columns",
            category=PluginCategory.TRANSFORM,
            description="Rename columns using a mapping or a function",
            icon="PencilLine",
            color="#3b82f6",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="mapping", label="Rename Mapping (JSON)", param_type="code", default="{}", required=False, description='e.g. {"old_name": "new_name"}'),
                ParamDefinition(name="function", label="Rename Function", param_type="select", default="none", options=["none", "lower", "upper", "strip", "snake_case"], required=False, description="Apply a function to all column names"),
                ParamDefinition(name="prefix", label="Add Prefix", param_type="string", default="", required=False, description="Prefix to add to all column names"),
                ParamDefinition(name="suffix", label="Add Suffix", param_type="string", default="", required=False, description="Suffix to add to all column names"),
            ],
        )

    @staticmethod
    def _to_snake(name: str) -> str:
        import re
        s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
        return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower().replace(" ", "_")

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        import json as _json
        lf = inputs["input"]
        p = self.params

        mapping_str = p.get("mapping", "{}") or "{}"
        mapping = _json.loads(mapping_str)
        if mapping:
            lf = lf.rename(mapping)

        func = p.get("function", "none")
        if func and func != "none":
            fn_map = {
                "lower": str.lower,
                "upper": str.upper,
                "strip": str.strip,
                "snake_case": self._to_snake,
            }
            lf = lf.rename(fn_map[func])

        prefix = p.get("prefix", "")
        suffix = p.get("suffix", "")
        if prefix or suffix:
            lf = lf.rename(lambda c: f"{prefix}{c}{suffix}")

        return {"output": lf}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        import json as _json
        inv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        lines = []

        mapping_str = p.get("mapping", "{}") or "{}"
        mapping = _json.loads(mapping_str)
        if mapping:
            lines.append(f"{var} = {inv}.rename({mapping!r})")
            inv = var

        func = p.get("function", "none")
        if func and func != "none":
            fn_code = {"lower": "str.lower", "upper": "str.upper", "strip": "str.strip", "snake_case": "lambda c: re.sub(r'([a-z0-9])([A-Z])', r'\\1_\\2', re.sub(r'([A-Z]+)([A-Z][a-z])', r'\\1_\\2', c)).lower()"}
            lines.append(f"{var} = {inv}.rename({fn_code[func]})")
            inv = var

        prefix = p.get("prefix", "")
        suffix = p.get("suffix", "")
        if prefix or suffix:
            lines.append(f"{var} = {inv}.rename(lambda c: f\"{prefix}{{c}}{suffix}\")")

        if not lines:
            lines.append(f"{var} = {inv}")

        return "\n".join(lines), {"output": var}
