from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class Sample(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="sample",
            name="Sample / Head / Tail",
            category=PluginCategory.TRANSFORM,
            description="Take the first N, last N, or a random sample of rows",
            icon="ListFilter",
            color="#6366f1",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="mode", label="Mode", param_type="select", default="head", options=["head", "tail", "sample_n", "sample_fraction"], required=False, description="head = first N, tail = last N, sample_n = random N, sample_fraction = random %"),
                ParamDefinition(name="n", label="N rows", param_type="number", default=100, required=False, description="Number of rows (for head/tail/sample_n)"),
                ParamDefinition(name="fraction", label="Fraction", param_type="string", default="0.1", required=False, description="Fraction 0.0-1.0 (for sample_fraction)"),
                ParamDefinition(name="with_replacement", label="With replacement", param_type="boolean", default=False, required=False, description="Sample with replacement (for sample modes)"),
                ParamDefinition(name="shuffle", label="Shuffle result", param_type="boolean", default=False, required=False, description="Shuffle the output rows"),
                ParamDefinition(name="seed", label="Random seed", param_type="number", default=0, required=False, description="Seed for reproducible sampling (0 = random)"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params
        mode = p.get("mode", "head")
        n = int(p.get("n", 100))
        seed = int(p.get("seed", 0)) or None

        if mode == "head":
            result = lf.head(n)
        elif mode == "tail":
            result = lf.tail(n)
        elif mode == "sample_n":
            df = lf.collect()
            result = df.sample(n=min(n, len(df)), with_replacement=p.get("with_replacement", False), seed=seed, shuffle=p.get("shuffle", False)).lazy()
        else:  # sample_fraction
            frac = float(p.get("fraction", 0.1))
            df = lf.collect()
            result = df.sample(fraction=frac, with_replacement=p.get("with_replacement", False), seed=seed, shuffle=p.get("shuffle", False)).lazy()

        if p.get("shuffle", False) and mode in ("head", "tail"):
            df = result.collect()
            result = df.sample(fraction=1.0, shuffle=True, seed=seed).lazy()

        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        mode = p.get("mode", "head")
        n = int(p.get("n", 100))
        if mode == "head":
            code = f"{var} = {iv}.head({n})"
        elif mode == "tail":
            code = f"{var} = {iv}.tail({n})"
        elif mode == "sample_n":
            code = f"{var} = {iv}.collect().sample(n={n}).lazy()"
        else:
            frac = float(p.get("fraction", 0.1))
            code = f"{var} = {iv}.collect().sample(fraction={frac}).lazy()"
        return code, {"output": var}
