"""Pipeline execution engine – topological execution with streaming support."""

from __future__ import annotations

import time
from typing import Any

import polars as pl

from core.models import (
    ExecutionStatus,
    FlowEdge,
    FlowNode,
    NodeExecutionResult,
    Pipeline,
    PipelineExecutionResult,
)
from core.plugin_manager import get_plugin


def _topo_sort(nodes: list[FlowNode], edges: list[FlowEdge]) -> list[str]:
    """Return node ids in topological (execution) order."""
    graph: dict[str, list[str]] = {n.id: [] for n in nodes}
    in_degree: dict[str, int] = {n.id: 0 for n in nodes}
    for e in edges:
        graph[e.source].append(e.target)
        in_degree[e.target] = in_degree.get(e.target, 0) + 1

    queue = [nid for nid, deg in in_degree.items() if deg == 0]
    order: list[str] = []
    while queue:
        nid = queue.pop(0)
        order.append(nid)
        for child in graph.get(nid, []):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(order) != len(nodes):
        raise ValueError("Pipeline contains a cycle")
    return order


def execute_pipeline(pipeline: Pipeline) -> PipelineExecutionResult:
    """Execute all nodes in topological order, passing LazyFrames between them."""
    order = _topo_sort(pipeline.nodes, pipeline.edges)
    node_map: dict[str, FlowNode] = {n.id: n for n in pipeline.nodes}

    # Collect outputs keyed by (node_id, port_name)
    outputs: dict[tuple[str, str], pl.LazyFrame] = {}
    results: dict[str, NodeExecutionResult] = {}

    for nid in order:
        node = node_map[nid]
        plugin_cls = get_plugin(node.data.plugin_id)
        if plugin_cls is None:
            results[nid] = NodeExecutionResult(
                node_id=nid,
                status=ExecutionStatus.ERROR,
                error=f"Unknown plugin: {node.data.plugin_id}",
            )
            continue

        # Gather inputs from upstream edges
        input_frames: dict[str, pl.LazyFrame] = {}
        for edge in pipeline.edges:
            if edge.target == nid:
                key = (edge.source, edge.source_handle)
                if key in outputs:
                    input_frames[edge.target_handle] = outputs[key]

        plugin = plugin_cls(node.data.params)
        t0 = time.perf_counter()
        try:
            out = plugin.execute(input_frames)
            duration = (time.perf_counter() - t0) * 1000

            # Store outputs
            for port_name, lf in out.items():
                outputs[(nid, port_name)] = lf

            # Build preview from first output
            if out:
                first_lf = next(iter(out.values()))
                collected = first_lf.collect(streaming=True)
                preview = collected.head(5).to_dicts()
                results[nid] = NodeExecutionResult(
                    node_id=nid,
                    status=ExecutionStatus.SUCCESS,
                    rows=collected.height,
                    columns=collected.width,
                    preview=preview,
                    duration_ms=round(duration, 2),
                )
            else:
                results[nid] = NodeExecutionResult(
                    node_id=nid,
                    status=ExecutionStatus.SUCCESS,
                    duration_ms=round(duration, 2),
                )
        except Exception as exc:
            duration = (time.perf_counter() - t0) * 1000
            results[nid] = NodeExecutionResult(
                node_id=nid,
                status=ExecutionStatus.ERROR,
                error=str(exc),
                duration_ms=round(duration, 2),
            )

    overall = ExecutionStatus.SUCCESS
    if any(r.status == ExecutionStatus.ERROR for r in results.values()):
        overall = ExecutionStatus.ERROR

    return PipelineExecutionResult(status=overall, node_results=results)
