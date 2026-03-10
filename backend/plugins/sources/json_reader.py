from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import SourcePlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class JSONReader(SourcePlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="json_reader",
            name="Read JSON",
            category=PluginCategory.SOURCE,
            description="Read data from a JSON or NDJSON file",
            icon="Braces",
            color="#22c55e",
            inputs=[],
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="file_path", label="File Path", param_type="string", required=True, description="Path to the JSON / NDJSON file"),
                ParamDefinition(name="format", label="Format", param_type="select", default="json", options=["json", "ndjson"], required=False, description="json = single array/object, ndjson = one JSON per line"),
                ParamDefinition(name="infer_schema_length", label="Infer Schema Length", param_type="number", default=100, required=False, description="Number of rows to infer types from"),
                # NDJSON-specific options
                ParamDefinition(name="batch_size", label="Batch Size (NDJSON)", param_type="number", default=1024, required=False, description="Rows per batch when reading NDJSON"),
                ParamDefinition(name="n_rows", label="Max Rows (NDJSON)", param_type="number", default=0, required=False, description="Maximum rows to read (0 = all). NDJSON only"),
                ParamDefinition(name="low_memory", label="Low Memory (NDJSON)", param_type="boolean", default=False, required=False, description="Reduce memory at cost of speed. NDJSON only"),
                ParamDefinition(name="rechunk", label="Rechunk (NDJSON)", param_type="boolean", default=False, required=False, description="Reallocate to contiguous memory. NDJSON only"),
                ParamDefinition(name="ignore_errors", label="Ignore Errors (NDJSON)", param_type="boolean", default=False, required=False, description="Skip invalid lines. NDJSON only"),
                ParamDefinition(name="row_index_name", label="Row Index Column", param_type="string", default="", required=False, description="Add a row index column with this name (NDJSON only)"),
                ParamDefinition(name="row_index_offset", label="Row Index Offset", param_type="number", default=0, required=False, description="Starting value for row index"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        p = self.params
        fmt = p.get("format", "json")

        if fmt == "ndjson":
            kwargs: dict[str, Any] = dict(
                source=p["file_path"],
                infer_schema_length=int(p.get("infer_schema_length", 100)),
                batch_size=int(p.get("batch_size", 1024)),
                low_memory=p.get("low_memory", False),
                rechunk=p.get("rechunk", False),
                ignore_errors=p.get("ignore_errors", False),
            )
            n_rows = int(p.get("n_rows", 0))
            if n_rows > 0:
                kwargs["n_rows"] = n_rows
            ri = p.get("row_index_name", "")
            if ri:
                kwargs["row_index_name"] = ri
                kwargs["row_index_offset"] = int(p.get("row_index_offset", 0))
            df = pl.read_ndjson(**kwargs)
        else:
            df = pl.read_json(
                p["file_path"],
                infer_schema_length=int(p.get("infer_schema_length", 100)),
            )
        return {"output": df.lazy()}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        p = params
        var = "__OUT__"
        fmt = p.get("format", "json")

        if fmt == "ndjson":
            args = [f"{p['file_path']!r}"]
            isl = int(p.get("infer_schema_length", 100))
            if isl != 100:
                args.append(f"infer_schema_length={isl}")
            bs = int(p.get("batch_size", 1024))
            if bs != 1024:
                args.append(f"batch_size={bs}")
            nr = int(p.get("n_rows", 0))
            if nr > 0:
                args.append(f"n_rows={nr}")
            if p.get("low_memory", False):
                args.append("low_memory=True")
            if p.get("rechunk", False):
                args.append("rechunk=True")
            if p.get("ignore_errors", False):
                args.append("ignore_errors=True")
            ri = p.get("row_index_name", "")
            if ri:
                args.append(f"row_index_name={ri!r}")
                rio = int(p.get("row_index_offset", 0))
                if rio:
                    args.append(f"row_index_offset={rio}")
            code = f"{var} = pl.read_ndjson(\n    {',\n    '.join(args)}\n).lazy()"
        else:
            args = [f"{p['file_path']!r}"]
            isl = int(p.get("infer_schema_length", 100))
            if isl != 100:
                args.append(f"infer_schema_length={isl}")
            code = f"{var} = pl.read_json({', '.join(args)}).lazy()"
        return code, {"output": var}
