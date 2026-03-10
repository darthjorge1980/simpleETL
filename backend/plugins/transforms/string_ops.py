from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class StringOps(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="string_ops",
            name="String Operations",
            category=PluginCategory.TRANSFORM,
            description="Apply string transformations to columns",
            icon="Type",
            color="#14b8a6",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="column", label="Column", param_type="string", required=True, description="Column to transform"),
                ParamDefinition(name="operation", label="Operation", param_type="select", default="to_uppercase", options=[
                    "to_uppercase", "to_lowercase", "strip", "strip_prefix", "strip_suffix",
                    "lstrip", "rstrip", "pad_start", "pad_end",
                    "replace", "replace_all", "slice", "contains", "starts_with", "ends_with",
                    "extract", "count_matches", "len_chars", "len_bytes",
                    "to_titlecase", "reverse", "zfill",
                ], required=True, description="String operation to apply"),
                ParamDefinition(name="output_column", label="Output column", param_type="string", default="", required=False, description="Output column name (empty = overwrite)"),
                ParamDefinition(name="arg1", label="Argument 1", param_type="string", default="", required=False, description="Pattern / prefix / suffix / pad char / offset depending on operation"),
                ParamDefinition(name="arg2", label="Argument 2", param_type="string", default="", required=False, description="Replacement / length depending on operation"),
                ParamDefinition(name="literal", label="Literal match", param_type="boolean", default=True, required=False, description="Use literal string matching (not regex) for replace/contains/extract"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params
        col = p["column"]
        op = p.get("operation", "to_uppercase")
        out = p.get("output_column", "") or col
        arg1 = p.get("arg1", "")
        arg2 = p.get("arg2", "")
        literal = p.get("literal", True)

        expr = pl.col(col)
        if op == "to_uppercase":
            expr = expr.str.to_uppercase()
        elif op == "to_lowercase":
            expr = expr.str.to_lowercase()
        elif op == "to_titlecase":
            expr = expr.str.to_titlecase()
        elif op == "strip":
            expr = expr.str.strip_chars(arg1 or None)
        elif op == "lstrip":
            expr = expr.str.strip_chars_start(arg1 or None)
        elif op == "rstrip":
            expr = expr.str.strip_chars_end(arg1 or None)
        elif op == "strip_prefix":
            expr = expr.str.strip_prefix(arg1)
        elif op == "strip_suffix":
            expr = expr.str.strip_suffix(arg1)
        elif op == "pad_start":
            expr = expr.str.pad_start(int(arg2 or 10), arg1 or " ")
        elif op == "pad_end":
            expr = expr.str.pad_end(int(arg2 or 10), arg1 or " ")
        elif op == "replace":
            expr = expr.str.replace(arg1, arg2, literal=literal)
        elif op == "replace_all":
            expr = expr.str.replace_all(arg1, arg2, literal=literal)
        elif op == "slice":
            offset = int(arg1 or 0)
            length = int(arg2) if arg2 else None
            expr = expr.str.slice(offset, length)
        elif op == "contains":
            expr = expr.str.contains(arg1, literal=literal)
        elif op == "starts_with":
            expr = expr.str.starts_with(arg1)
        elif op == "ends_with":
            expr = expr.str.ends_with(arg1)
        elif op == "extract":
            expr = expr.str.extract(arg1, int(arg2 or 1))
        elif op == "count_matches":
            expr = expr.str.count_matches(arg1, literal=literal)
        elif op == "len_chars":
            expr = expr.str.len_chars()
        elif op == "len_bytes":
            expr = expr.str.len_bytes()
        elif op == "reverse":
            expr = expr.str.reverse()
        elif op == "zfill":
            expr = expr.str.zfill(int(arg1 or 10))

        result = lf.with_columns(expr.alias(out))
        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        col = p["column"]
        op = p.get("operation", "to_uppercase")
        out = p.get("output_column", "") or col
        code = f"{var} = {iv}.with_columns(pl.col({col!r}).str.{op}().alias({out!r}))"
        return code, {"output": var}
