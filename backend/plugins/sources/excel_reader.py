from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import SourcePlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class ExcelReader(SourcePlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="excel_reader",
            name="Read Excel",
            category=PluginCategory.SOURCE,
            description="Read data from an Excel (.xlsx/.xls/.ods) file",
            icon="Table",
            color="#22c55e",
            inputs=[],
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="file_path", label="File Path", param_type="string", required=True, description="Path to the Excel file (.xlsx, .xls, .ods)"),
                ParamDefinition(name="sheet_name", label="Sheet Name", param_type="string", default="", required=False, description="Name of the sheet to read (leave empty + set Sheet ID to use index)"),
                ParamDefinition(name="sheet_id", label="Sheet ID (index)", param_type="number", default=0, required=False, description="1-based sheet index. 0 = use sheet name instead"),
                ParamDefinition(name="table_name", label="Table Name", param_type="string", default="", required=False, description="Named table/range to read from (Excel tables)"),
                ParamDefinition(name="has_header", label="Has Header", param_type="boolean", default=True, required=False, description="First row contains column names"),
                ParamDefinition(name="columns", label="Columns (comma-separated)", param_type="string", default="", required=False, description="Read only these columns (leave empty = all)"),
                ParamDefinition(name="engine", label="Engine", param_type="select", default="calamine", options=["calamine", "openpyxl", "xlsx2csv"], required=False, description="Parser engine to use"),
                ParamDefinition(name="infer_schema_length", label="Infer Schema Length", param_type="number", default=100, required=False, description="Number of rows used to infer column types"),
                ParamDefinition(name="drop_empty_rows", label="Drop Empty Rows", param_type="boolean", default=True, required=False, description="Remove completely empty rows"),
                ParamDefinition(name="drop_empty_cols", label="Drop Empty Columns", param_type="boolean", default=True, required=False, description="Remove completely empty columns"),
                ParamDefinition(name="raise_if_empty", label="Raise If Empty", param_type="boolean", default=True, required=False, description="Raise an error if the result is empty"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        p = self.params
        kwargs: dict[str, Any] = dict(
            source=p["file_path"],
            has_header=p.get("has_header", True),
            engine=p.get("engine", "calamine"),
            infer_schema_length=int(p.get("infer_schema_length", 100)),
            drop_empty_rows=p.get("drop_empty_rows", True),
            drop_empty_cols=p.get("drop_empty_cols", True),
            raise_if_empty=p.get("raise_if_empty", True),
        )

        sheet_name = p.get("sheet_name", "")
        sheet_id = int(p.get("sheet_id", 0))
        if sheet_name:
            kwargs["sheet_name"] = sheet_name
        elif sheet_id > 0:
            kwargs["sheet_id"] = sheet_id

        table_name = p.get("table_name", "")
        if table_name:
            kwargs["table_name"] = table_name

        cols_str = p.get("columns", "")
        if cols_str:
            kwargs["columns"] = [c.strip() for c in cols_str.split(",") if c.strip()]

        df = pl.read_excel(**kwargs)
        return {"output": df.lazy()}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        p = params
        var = "__OUT__"
        args = [f"{p['file_path']!r}"]

        sheet_name = p.get("sheet_name", "")
        sheet_id = int(p.get("sheet_id", 0))
        if sheet_name:
            args.append(f"sheet_name={sheet_name!r}")
        elif sheet_id > 0:
            args.append(f"sheet_id={sheet_id}")

        table_name = p.get("table_name", "")
        if table_name:
            args.append(f"table_name={table_name!r}")

        if not p.get("has_header", True):
            args.append("has_header=False")

        cols_str = p.get("columns", "")
        if cols_str:
            cols = [c.strip() for c in cols_str.split(",") if c.strip()]
            args.append(f"columns={cols!r}")

        eng = p.get("engine", "calamine")
        if eng != "calamine":
            args.append(f"engine={eng!r}")

        isl = int(p.get("infer_schema_length", 100))
        if isl != 100:
            args.append(f"infer_schema_length={isl}")

        if not p.get("drop_empty_rows", True):
            args.append("drop_empty_rows=False")
        if not p.get("drop_empty_cols", True):
            args.append("drop_empty_cols=False")
        if not p.get("raise_if_empty", True):
            args.append("raise_if_empty=False")

        code = f"{var} = pl.read_excel(\n    {',\\n    '.join(args)}\n).lazy()"
        return code, {"output": var}
