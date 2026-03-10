from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo, PortDefinition, PortType


@register
class Lookup(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="lookup",
            name="Lookup",
            category=PluginCategory.TRANSFORM,
            description="Enrich rows by looking up values from a reference table (left join)",
            icon="Search",
            color="#0ea5e9",
            inputs=[
                PortDefinition(name="input", port_type=PortType.DATAFRAME),
                PortDefinition(name="reference", port_type=PortType.DATAFRAME),
            ],
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="input_key", label="Input Key(s)", param_type="string", required=True, description="Key column(s) in input (comma-separated)"),
                ParamDefinition(name="ref_key", label="Reference Key(s)", param_type="string", required=True, description="Key column(s) in reference (comma-separated)"),
                ParamDefinition(name="select_cols", label="Columns to bring", param_type="string", default="", required=False, description="Columns to keep from reference (comma-separated, empty = all)"),
                ParamDefinition(name="suffix", label="Suffix", param_type="string", default="_ref", required=False, description="Suffix for duplicate column names"),
                ParamDefinition(name="reject_unmatched", label="Reject unmatched", param_type="boolean", default=False, required=False, description="If true, only keep rows that found a match (inner join)"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        ref = inputs["reference"]
        p = self.params

        input_on = [c.strip() for c in p["input_key"].split(",") if c.strip()]
        ref_on = [c.strip() for c in p["ref_key"].split(",") if c.strip()]

        select_cols = p.get("select_cols", "")
        if select_cols and select_cols.strip():
            keep = [c.strip() for c in select_cols.split(",") if c.strip()]
            ref = ref.select(ref_on + [c for c in keep if c not in ref_on])

        how = "inner" if p.get("reject_unmatched", False) else "left"

        result = lf.join(
            ref,
            left_on=input_on if len(input_on) > 1 else input_on[0],
            right_on=ref_on if len(ref_on) > 1 else ref_on[0],
            how=how,
            suffix=p.get("suffix", "_ref"),
        )
        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        rv = input_vars.get("reference", "df_ref")
        p = params
        var = "__OUT__"
        input_on = [c.strip() for c in p["input_key"].split(",") if c.strip()]
        ref_on = [c.strip() for c in p["ref_key"].split(",") if c.strip()]
        lo = input_on if len(input_on) > 1 else input_on[0]
        ro = ref_on if len(ref_on) > 1 else ref_on[0]
        how = "inner" if p.get("reject_unmatched", False) else "left"
        code = f"{var} = {iv}.join({rv}, left_on={lo!r}, right_on={ro!r}, how={how!r}, suffix={p.get('suffix', '_ref')!r})"
        return code, {"output": var}
