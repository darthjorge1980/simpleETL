from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo, PortDefinition, PortType


@register
class ChangeCapture(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="change_capture",
            name="Change Capture (CDC)",
            category=PluginCategory.TRANSFORM,
            description="Compare current vs previous dataset to detect inserts, updates, and deletes",
            icon="GitCompare",
            color="#dc2626",
            inputs=[
                PortDefinition(name="current", port_type=PortType.DATAFRAME),
                PortDefinition(name="previous", port_type=PortType.DATAFRAME),
            ],
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="key_columns", label="Key column(s)", param_type="string", required=True, description="Primary key columns (comma-separated)"),
                ParamDefinition(name="compare_columns", label="Compare column(s)", param_type="string", default="", required=False, description="Columns to check for changes (comma-separated, empty = all non-key)"),
                ParamDefinition(name="change_column", label="Change type column", param_type="string", default="_change_type", required=False, description="Name of column indicating I/U/D"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        curr = inputs["current"].collect()
        prev = inputs["previous"].collect()
        p = self.params

        keys = [c.strip() for c in p["key_columns"].split(",") if c.strip()]
        change_col = p.get("change_column", "_change_type") or "_change_type"

        compare_str = p.get("compare_columns", "")
        if compare_str and compare_str.strip():
            compare = [c.strip() for c in compare_str.split(",") if c.strip()]
        else:
            compare = [c for c in curr.columns if c not in keys]

        # Inserts: in current but not in previous
        inserts = curr.join(prev.select(keys), on=keys, how="anti")
        inserts = inserts.with_columns(pl.lit("INSERT").alias(change_col))

        # Deletes: in previous but not in current
        deletes = prev.join(curr.select(keys), on=keys, how="anti")
        deletes = deletes.with_columns(pl.lit("DELETE").alias(change_col))

        # Updates: same key, different values
        matched = curr.join(prev, on=keys, how="inner", suffix="_prev")
        if compare:
            change_expr = pl.lit(False)
            for c in compare:
                prev_col = f"{c}_prev"
                if prev_col in matched.columns:
                    change_expr = change_expr | (pl.col(c) != pl.col(prev_col))
            updates = matched.filter(change_expr).select(curr.columns)
            updates = updates.with_columns(pl.lit("UPDATE").alias(change_col))
        else:
            updates = pl.DataFrame(schema={**{c: curr.schema[c] for c in curr.columns}, change_col: pl.Utf8})

        result = pl.concat([inserts, updates, deletes], how="diagonal_relaxed")
        return {"output": result.lazy()}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        cv = input_vars.get("current", "df_current")
        pv = input_vars.get("previous", "df_previous")
        p = params
        var = "__OUT__"
        keys = [c.strip() for c in p["key_columns"].split(",") if c.strip()]
        cc = p.get("change_column", "_change_type") or "_change_type"
        code = f"# CDC: compare {cv} vs {pv} on keys {keys}\n"
        code += f"_curr, _prev = {cv}.collect(), {pv}.collect()\n"
        code += f"_ins = _curr.join(_prev.select({keys!r}), on={keys!r}, how='anti').with_columns(pl.lit('INSERT').alias({cc!r}))\n"
        code += f"_del = _prev.join(_curr.select({keys!r}), on={keys!r}, how='anti').with_columns(pl.lit('DELETE').alias({cc!r}))\n"
        code += f"{var} = pl.concat([_ins, _del], how='diagonal_relaxed').lazy()"
        return code, {"output": var}
