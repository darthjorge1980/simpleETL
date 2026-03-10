from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import DestinationPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class JSONWriter(DestinationPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="json_writer",
            name="Write JSON",
            category=PluginCategory.DESTINATION,
            description="Write data to a JSON or NDJSON file",
            icon="FileDown",
            color="#f59e0b",
            inputs=cls._base_inputs(),
            outputs=[],
            params=[
                ParamDefinition(name="file_path", label="File Path", param_type="string", required=True, description="Output file path"),
                ParamDefinition(name="format", label="Format", param_type="select", default="json", options=["json", "ndjson"], required=False, description="json = single JSON array, ndjson = one JSON per line"),
                # NDJSON-specific options
                ParamDefinition(name="compression", label="Compression (NDJSON)", param_type="select", default="uncompressed", options=["uncompressed", "gzip", "zstd"], required=False, description="Compress the output. NDJSON only"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        df = lf.collect(streaming=True)
        p = self.params
        fmt = p.get("format", "json")

        if fmt == "ndjson":
            kwargs: dict[str, Any] = dict(file=p["file_path"])
            comp = p.get("compression", "uncompressed")
            if comp != "uncompressed":
                kwargs["compression"] = comp
            df.write_ndjson(**kwargs)
        else:
            df.write_json(p["file_path"])
        return {}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        inv = input_vars.get("input", "df_in")
        p = params
        fmt = p.get("format", "json")

        if fmt == "ndjson":
            args = [f"{p['file_path']!r}"]
            comp = p.get("compression", "uncompressed")
            if comp != "uncompressed":
                args.append(f"compression={comp!r}")
            code = f"{inv}.collect(streaming=True).write_ndjson({', '.join(args)})"
        else:
            code = f"{inv}.collect(streaming=True).write_json({p['file_path']!r})"
        return code, {}
