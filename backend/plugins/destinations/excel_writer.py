from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import DestinationPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class ExcelWriter(DestinationPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="excel_writer",
            name="Write Excel",
            category=PluginCategory.DESTINATION,
            description="Write data to an Excel (.xlsx) file",
            icon="FileDown",
            color="#f59e0b",
            inputs=cls._base_inputs(),
            outputs=[],
            params=[
                ParamDefinition(name="file_path", label="File Path", param_type="string", required=True, description="Output .xlsx file path"),
                ParamDefinition(name="sheet_name", label="Sheet Name", param_type="string", default="Sheet1", required=False, description="Worksheet name"),
                ParamDefinition(name="position", label="Start Position", param_type="string", default="A1", required=False, description="Top-left cell for the data (e.g. A1, B3)"),
                ParamDefinition(name="table_style", label="Table Style", param_type="select", default="none", options=["none", "Table Style Light 1", "Table Style Light 9", "Table Style Light 15", "Table Style Medium 1", "Table Style Medium 4", "Table Style Medium 9", "Table Style Dark 1", "Table Style Dark 3", "Table Style Dark 6"], required=False, description="Excel table style (none = no table formatting)"),
                ParamDefinition(name="table_name", label="Table Name", param_type="string", default="", required=False, description="Named table for the data range"),
                ParamDefinition(name="include_header", label="Include Header", param_type="boolean", default=True, required=False, description="Write column names as the first row"),
                ParamDefinition(name="autofilter", label="Auto Filter", param_type="boolean", default=True, required=False, description="Add auto-filter dropdowns to headers"),
                ParamDefinition(name="autofit", label="Auto-fit Column Widths", param_type="boolean", default=False, required=False, description="Automatically size columns to fit content"),
                ParamDefinition(name="float_precision", label="Float Precision", param_type="number", default=3, required=False, description="Number of decimal places for floats"),
                ParamDefinition(name="hide_gridlines", label="Hide Gridlines", param_type="boolean", default=False, required=False, description="Hide cell gridlines in the worksheet"),
                ParamDefinition(name="sheet_zoom", label="Sheet Zoom (%)", param_type="number", default=0, required=False, description="Zoom percentage (0 = default 100%)"),
                ParamDefinition(name="freeze_panes", label="Freeze Panes", param_type="string", default="", required=False, description="Cell reference to freeze at, e.g. A2 freezes first row"),
                ParamDefinition(name="hidden_columns", label="Hidden Columns", param_type="string", default="", required=False, description="Comma-separated column names to hide"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        df = lf.collect(streaming=True)
        p = self.params

        kwargs: dict[str, Any] = dict(
            workbook=p["file_path"],
            worksheet=p.get("sheet_name", "Sheet1"),
            position=p.get("position", "A1"),
            include_header=p.get("include_header", True),
            autofilter=p.get("autofilter", True),
            autofit=p.get("autofit", False),
            float_precision=int(p.get("float_precision", 3)),
            hide_gridlines=p.get("hide_gridlines", False),
        )

        ts = p.get("table_style", "none")
        if ts and ts != "none":
            kwargs["table_style"] = ts

        tn = p.get("table_name", "")
        if tn:
            kwargs["table_name"] = tn

        sz = int(p.get("sheet_zoom", 0))
        if sz > 0:
            kwargs["sheet_zoom"] = sz

        fp = p.get("freeze_panes", "")
        if fp:
            kwargs["freeze_panes"] = fp

        hc = p.get("hidden_columns", "")
        if hc:
            kwargs["hidden_columns"] = [c.strip() for c in hc.split(",") if c.strip()]

        df.write_excel(**kwargs)
        return {}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        inv = input_vars.get("input", "df_in")
        p = params
        args = [f"{p['file_path']!r}"]
        args.append(f"worksheet={p.get('sheet_name', 'Sheet1')!r}")

        pos = p.get("position", "A1")
        if pos != "A1":
            args.append(f"position={pos!r}")

        ts = p.get("table_style", "none")
        if ts and ts != "none":
            args.append(f"table_style={ts!r}")

        tn = p.get("table_name", "")
        if tn:
            args.append(f"table_name={tn!r}")

        if not p.get("include_header", True):
            args.append("include_header=False")
        if not p.get("autofilter", True):
            args.append("autofilter=False")
        if p.get("autofit", False):
            args.append("autofit=True")

        fp_val = int(p.get("float_precision", 3))
        if fp_val != 3:
            args.append(f"float_precision={fp_val}")

        if p.get("hide_gridlines", False):
            args.append("hide_gridlines=True")

        sz = int(p.get("sheet_zoom", 0))
        if sz > 0:
            args.append(f"sheet_zoom={sz}")

        freeze = p.get("freeze_panes", "")
        if freeze:
            args.append(f"freeze_panes={freeze!r}")

        hc = p.get("hidden_columns", "")
        if hc:
            cols = [c.strip() for c in hc.split(",") if c.strip()]
            args.append(f"hidden_columns={cols!r}")

        code = f"{inv}.collect(streaming=True).write_excel(\n    {',\n    '.join(args)}\n)"
        return code, {}
