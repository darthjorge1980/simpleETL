export type PluginCategory = "source" | "transform" | "destination";

export interface PortDefinition {
  name: string;
  port_type: string;
  required: boolean;
}

export interface ParamDefinition {
  name: string;
  label: string;
  param_type: string;
  default?: unknown;
  options?: string[];
  required: boolean;
  description: string;
}

export interface PluginInfo {
  id: string;
  name: string;
  category: PluginCategory;
  description: string;
  icon: string;
  color: string;
  inputs: PortDefinition[];
  outputs: PortDefinition[];
  params: ParamDefinition[];
}

export interface NodeData {
  plugin_id: string;
  params: Record<string, unknown>;
  label?: string;
  pluginInfo?: PluginInfo;
  executionResult?: NodeExecutionResult;
  [key: string]: unknown;
}

export interface NodeExecutionResult {
  node_id: string;
  status: "pending" | "running" | "success" | "error";
  rows: number;
  columns: number;
  preview: Record<string, unknown>[];
  error?: string;
  duration_ms: number;
}

export interface PipelineExecutionResult {
  status: "pending" | "running" | "success" | "error";
  node_results: Record<string, NodeExecutionResult>;
  generated_code?: string;
}
