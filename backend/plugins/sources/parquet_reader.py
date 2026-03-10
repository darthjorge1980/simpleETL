from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import SourcePlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class ParquetReader(SourcePlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="parquet_reader",
            name="Read Parquet",
            category=PluginCategory.SOURCE,
            description="Read data from a Parquet file (ideal for large datasets, streaming)",
            icon="Database",
            color="#22c55e",
            inputs=[],
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="file_path", label="File Path", param_type="string", required=True, description="Path to the Parquet file (supports glob patterns)"),
                ParamDefinition(name="n_rows", label="Max Rows", param_type="number", default=0, required=False, description="Maximum rows to read (0 = all)"),
                ParamDefinition(name="parallel", label="Parallel Strategy", param_type="select", default="auto", options=["auto", "columns", "row_groups", "prefiltered", "none"], required=False, description="Parallelization strategy"),
                ParamDefinition(name="use_statistics", label="Use Statistics", param_type="boolean", default=True, required=False, description="Use Parquet statistics for query optimization"),
                ParamDefinition(name="hive_partitioning", label="Hive Partitioning", param_type="select", default="auto", options=["auto", "true", "false"], required=False, description="Read Hive-style partitioned directories"),
                ParamDefinition(name="try_parse_hive_dates", label="Parse Hive Dates", param_type="boolean", default=True, required=False, description="Try to parse dates in Hive partition columns"),
                ParamDefinition(name="glob", label="Glob Patterns", param_type="boolean", default=True, required=False, description="Expand glob patterns in file path"),
                ParamDefinition(name="rechunk", label="Rechunk", param_type="boolean", default=False, required=False, description="Reallocate to contiguous memory after reading"),
                ParamDefinition(name="low_memory", label="Low Memory Mode", param_type="boolean", default=False, required=False, description="Reduce memory usage at cost of performance"),
                ParamDefinition(name="cache", label="Cache", param_type="boolean", default=True, required=False, description="Cache the scan result for reuse"),
                ParamDefinition(name="row_index_name", label="Row Index Column", param_type="string", default="", required=False, description="Add a row index column with this name"),
                ParamDefinition(name="row_index_offset", label="Row Index Offset", param_type="number", default=0, required=False, description="Starting value for row index"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        p = self.params
        kwargs: dict[str, Any] = dict(
            source=p["file_path"],
            use_statistics=p.get("use_statistics", True),
            try_parse_hive_dates=p.get("try_parse_hive_dates", True),
            glob=p.get("glob", True),
            rechunk=p.get("rechunk", False),
            low_memory=p.get("low_memory", False),
            cache=p.get("cache", True),
        )

        parallel = p.get("parallel", "auto")
        kwargs["parallel"] = parallel

        hive = p.get("hive_partitioning", "auto")
        if hive == "true":
            kwargs["hive_partitioning"] = True
        elif hive == "false":
            kwargs["hive_partitioning"] = False

        n_rows = int(p.get("n_rows", 0))
        if n_rows > 0:
            kwargs["n_rows"] = n_rows

        ri = p.get("row_index_name", "")
        if ri:
            kwargs["row_index_name"] = ri
            kwargs["row_index_offset"] = int(p.get("row_index_offset", 0))

        lf = pl.scan_parquet(**kwargs)
        return {"output": lf}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        p = params
        var = "__OUT__"
        args = [f"{p['file_path']!r}"]

        nr = int(p.get("n_rows", 0))
        if nr > 0:
            args.append(f"n_rows={nr}")

        par = p.get("parallel", "auto")
        if par != "auto":
            args.append(f"parallel={par!r}")

        if not p.get("use_statistics", True):
            args.append("use_statistics=False")

        hive = p.get("hive_partitioning", "auto")
        if hive == "true":
            args.append("hive_partitioning=True")
        elif hive == "false":
            args.append("hive_partitioning=False")

        if not p.get("try_parse_hive_dates", True):
            args.append("try_parse_hive_dates=False")
        if not p.get("glob", True):
            args.append("glob=False")
        if p.get("rechunk", False):
            args.append("rechunk=True")
        if p.get("low_memory", False):
            args.append("low_memory=True")
        if not p.get("cache", True):
            args.append("cache=False")

        ri = p.get("row_index_name", "")
        if ri:
            args.append(f"row_index_name={ri!r}")
            rio = int(p.get("row_index_offset", 0))
            if rio:
                args.append(f"row_index_offset={rio}")

        code = f"{var} = pl.scan_parquet(\n    {',\n    '.join(args)}\n)"
        return code, {"output": var}
