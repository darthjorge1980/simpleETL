import type { PluginInfo, PipelineExecutionResult } from "@/types";

const BASE = "/api";

export async function fetchPlugins(): Promise<PluginInfo[]> {
  const res = await fetch(`${BASE}/plugins`);
  if (!res.ok) throw new Error("Failed to fetch plugins");
  return res.json();
}

export interface PipelinePayload {
  nodes: {
    id: string;
    type: string;
    position: { x: number; y: number };
    data: { plugin_id: string; params: Record<string, unknown> };
  }[];
  edges: {
    id: string;
    source: string;
    target: string;
    source_handle: string;
    target_handle: string;
  }[];
}

export async function executePipeline(
  pipeline: PipelinePayload
): Promise<PipelineExecutionResult> {
  const res = await fetch(`${BASE}/execute`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(pipeline),
  });
  if (!res.ok) {
    const detail = await res.text();
    throw new Error(detail);
  }
  return res.json();
}

export async function generateCode(
  pipeline: PipelinePayload,
  language = "python"
): Promise<{ language: string; code: string }> {
  const res = await fetch(`${BASE}/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ pipeline, language }),
  });
  if (!res.ok) throw new Error("Code generation failed");
  return res.json();
}

export async function savePipeline(
  name: string,
  pipeline: PipelinePayload
): Promise<void> {
  const res = await fetch(`${BASE}/pipelines/${encodeURIComponent(name)}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(pipeline),
  });
  if (!res.ok) throw new Error("Failed to save pipeline");
}

export async function listPipelines(): Promise<string[]> {
  const res = await fetch(`${BASE}/pipelines`);
  if (!res.ok) throw new Error("Failed to list pipelines");
  return res.json();
}

export async function loadPipeline(name: string): Promise<PipelinePayload> {
  const res = await fetch(`${BASE}/pipelines/${encodeURIComponent(name)}`);
  if (!res.ok) throw new Error("Failed to load pipeline");
  return res.json();
}
