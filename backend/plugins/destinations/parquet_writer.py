from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import DestinationPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class ParquetWriter(DestinationPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="parquet_writer",
            name="Write Parquet",
            category=PluginCategory.DESTINATION,
            description="Write data to a Parquet file (efficient columnar format for large data)",
            icon="FileDown",
            color="#f59e0b",
            inputs=cls._base_inputs(),
            outputs=[],
            params=[
                ParamDefinition(name="file_path", label="File Path", param_type="string", required=True, description="Output Parquet file path"),
                ParamDefinition(name="compression", label="Compression", param_type="select", default="zstd", options=["zstd", "snappy", "gzip", "lz4", "uncompressed"], required=False, description="Compression algorithm"),
                ParamDefinition(name="compression_level", label="Compression Level", param_type="number", default=0, required=False, description="Compression level (0 = library default). Range depends on algorithm"),
                ParamDefinition(name="statistics", label="Write Statistics", param_type="boolean", default=True, required=False, description="Write column statistics (min/max/null count) for query optimization"),
                ParamDefinition(name="row_group_size", label="Row Group Size", param_type="number", default=0, required=False, description="Rows per row group (0 = default)"),
                ParamDefinition(name="data_page_size", label="Data Page Size", param_type="number", default=0, required=False, description="Target size in bytes per data page (0 = default)"),
                ParamDefinition(name="partition_by", label="Partition By (comma-separated)", param_type="string", default="", required=False, description="Columns to use for Hive-style partitioning (writes to directories)"),
                ParamDefinition(name="mkdir", label="Create Directories", param_type="boolean", default=False, required=False, description="Create parent directories if they don't exist"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        df = lf.collect(streaming=True)
        p = self.params

        kwargs: dict[str, Any] = dict(
            file=p["file_path"],
            compression=p.get("compression", "zstd"),
            statistics=p.get("statistics", True),
            mkdir=p.get("mkdir", False),
        )

        cl = int(p.get("compression_level", 0))
        if cl > 0:
            kwargs["compression_level"] = cl

        rgs = int(p.get("row_group_size", 0))
        if rgs > 0:
            kwargs["row_group_size"] = rgs

        dps = int(p.get("data_page_size", 0))
        if dps > 0:
            kwargs["data_page_size"] = dps

        part = p.get("partition_by", "")
        if part:
            kwargs["partition_by"] = [c.strip() for c in part.split(",") if c.strip()]

        df.write_parquet(**kwargs)
        return {}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        inv = input_vars.get("input", "df_in")
        p = params
        args = [f"{p['file_path']!r}"]
        args.append(f"compression={p.get('compression', 'zstd')!r}")

        cl = int(p.get("compression_level", 0))
        if cl > 0:
            args.append(f"compression_level={cl}")

        if not p.get("statistics", True):
            args.append("statistics=False")

        rgs = int(p.get("row_group_size", 0))
        if rgs > 0:
            args.append(f"row_group_size={rgs}")

        dps = int(p.get("data_page_size", 0))
        if dps > 0:
            args.append(f"data_page_size={dps}")

        part = p.get("partition_by", "")
        if part:
            cols = [c.strip() for c in part.split(",") if c.strip()]
            args.append(f"partition_by={cols!r}")

        if p.get("mkdir", False):
            args.append("mkdir=True")

        code = f"{inv}.collect(streaming=True).write_parquet(\n    {',\n    '.join(args)}\n)"
        return code, {}
