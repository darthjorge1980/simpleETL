from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import SourcePlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class RowGenerator(SourcePlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="row_generator",
            name="Row Generator",
            category=PluginCategory.SOURCE,
            description="Generate test rows with sequential numbers and optional columns",
            icon="Table2",
            color="#6366f1",
            inputs=[],
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="num_rows", label="Number of rows", param_type="number", default=100, required=True, description="How many rows to generate"),
                ParamDefinition(name="columns_json", label="Columns (JSON)", param_type="code", default='[{"name":"id","type":"sequence"},{"name":"value","type":"random_float"}]', required=False, description='Column definitions: [{"name":"col","type":"sequence|random_int|random_float|constant|uuid","value":"..."}]'),
                ParamDefinition(name="seed", label="Random seed", param_type="number", default=42, required=False, description="Seed for reproducible random data (0 = random)"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        import json as _json
        import random
        import uuid

        p = self.params
        n = int(p.get("num_rows", 100))
        seed_val = int(p.get("seed", 42))
        rng = random.Random(seed_val if seed_val else None)

        cols_json = p.get("columns_json", "[]") or "[]"
        col_defs = _json.loads(cols_json)

        data: dict[str, list] = {}
        for cd in col_defs:
            name = cd["name"]
            ctype = cd.get("type", "sequence")
            val = cd.get("value", "")

            if ctype == "sequence":
                start = int(val) if val else 1
                data[name] = list(range(start, start + n))
            elif ctype == "random_int":
                lo, hi = 0, 1000
                if val and "," in val:
                    parts = val.split(",")
                    lo, hi = int(parts[0]), int(parts[1])
                data[name] = [rng.randint(lo, hi) for _ in range(n)]
            elif ctype == "random_float":
                lo, hi = 0.0, 1.0
                if val and "," in val:
                    parts = val.split(",")
                    lo, hi = float(parts[0]), float(parts[1])
                data[name] = [rng.uniform(lo, hi) for _ in range(n)]
            elif ctype == "constant":
                data[name] = [val] * n
            elif ctype == "uuid":
                data[name] = [str(uuid.uuid4()) for _ in range(n)]
            else:
                data[name] = list(range(1, n + 1))

        if not data:
            data = {"row_num": list(range(1, n + 1))}

        df = pl.DataFrame(data)
        return {"output": df.lazy()}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        p = params
        var = "__OUT__"
        n = int(p.get("num_rows", 100))
        code = f"# Row Generator: {n} rows\n{var} = pl.DataFrame({{'row_num': range(1, {n + 1})}}).lazy()"
        return code, {"output": var}
