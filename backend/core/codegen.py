"""Code generation – converts a visual pipeline into a standalone Python script."""

from __future__ import annotations

from core.models import Pipeline, FlowEdge, FlowNode
from core.plugin_manager import get_plugin
from core.engine import _topo_sort


def generate_python(pipeline: Pipeline) -> str:
    """Generate a self-contained Python script that replicates the pipeline."""
    order = _topo_sort(pipeline.nodes, pipeline.edges)
    node_map: dict[str, FlowNode] = {n.id: n for n in pipeline.nodes}

    lines: list[str] = [
        '"""Auto-generated ETL pipeline by SimpleETL."""\n',
        "import polars as pl\n\n",
    ]

    var_counter = 0
    # Maps (node_id, port_name) → variable name
    var_map: dict[tuple[str, str], str] = {}

    for nid in order:
        node = node_map[nid]
        plugin_cls = get_plugin(node.data.plugin_id)
        if plugin_cls is None:
            lines.append(f"# ERROR: unknown plugin {node.data.plugin_id}\n")
            continue

        # Build input variable mapping
        input_vars: dict[str, str] = {}
        for edge in pipeline.edges:
            if edge.target == nid:
                key = (edge.source, edge.source_handle)
                if key in var_map:
                    input_vars[edge.target_handle] = var_map[key]

        code_snippet, output_vars = plugin_cls.generate_code(node.data.params, input_vars)
        # Assign output vars with unique names
        final_output_vars: dict[str, str] = {}
        for port, suggested in output_vars.items():
            var_counter += 1
            vname = f"df_{var_counter}"
            final_output_vars[port] = vname
            var_map[(nid, port)] = vname

        # Replace placeholder names in generated code
        for port, suggested in output_vars.items():
            code_snippet = code_snippet.replace(suggested, final_output_vars[port])

        lines.append(f"# Node: {node.data.plugin_id} ({nid})\n")
        lines.append(code_snippet + "\n\n")

    return "".join(lines)
