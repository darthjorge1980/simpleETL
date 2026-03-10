from __future__ import annotations

import enum
from typing import Any

from pydantic import BaseModel, Field


class PluginCategory(str, enum.Enum):
    SOURCE = "source"
    TRANSFORM = "transform"
    DESTINATION = "destination"


class PortType(str, enum.Enum):
    DATAFRAME = "dataframe"


class PortDefinition(BaseModel):
    name: str
    port_type: PortType = PortType.DATAFRAME
    required: bool = True


class ParamDefinition(BaseModel):
    name: str
    label: str
    param_type: str = "string"  # string, number, boolean, select, code, column_select
    default: Any = None
    options: list[str] | None = None
    required: bool = True
    description: str = ""


class PluginInfo(BaseModel):
    id: str
    name: str
    category: PluginCategory
    description: str = ""
    icon: str = ""
    color: str = "#6366f1"
    inputs: list[PortDefinition] = Field(default_factory=list)
    outputs: list[PortDefinition] = Field(default_factory=list)
    params: list[ParamDefinition] = Field(default_factory=list)


# ── Flow / Pipeline models ──────────────────────────────────────────

class NodePosition(BaseModel):
    x: float = 0
    y: float = 0


class NodeData(BaseModel):
    plugin_id: str
    params: dict[str, Any] = Field(default_factory=dict)


class FlowNode(BaseModel):
    id: str
    type: str  # source / transform / destination
    position: NodePosition
    data: NodeData


class FlowEdge(BaseModel):
    id: str
    source: str
    target: str
    source_handle: str = "output"
    target_handle: str = "input"


class Pipeline(BaseModel):
    nodes: list[FlowNode]
    edges: list[FlowEdge]


# ── Execution models ────────────────────────────────────────────────

class ExecutionStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"


class NodeExecutionResult(BaseModel):
    node_id: str
    status: ExecutionStatus
    rows: int = 0
    columns: int = 0
    preview: list[dict[str, Any]] = Field(default_factory=list)
    error: str | None = None
    duration_ms: float = 0


class PipelineExecutionResult(BaseModel):
    status: ExecutionStatus
    node_results: dict[str, NodeExecutionResult] = Field(default_factory=dict)
    generated_code: str | None = None


class CodeGenRequest(BaseModel):
    pipeline: Pipeline
    language: str = "python"
