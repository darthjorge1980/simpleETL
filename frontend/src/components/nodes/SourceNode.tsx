import { memo } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import type { NodeData } from "@/types";
import { useFlowStore } from "@/stores/flowStore";
import {
  FileText,
  Braces,
  Table,
  Table2,
  Database,
  Globe,
  HardDrive,
  Eye,
  type LucideIcon,
} from "lucide-react";

const iconMap: Record<string, LucideIcon> = {
  FileText,
  Braces,
  Table,
  Table2,
  Database,
  Globe,
  HardDrive,
};

function SourceNode({ id, data, selected }: NodeProps) {
  const d = data as unknown as NodeData;
  const Icon = iconMap[d.pluginInfo?.icon ?? ""] ?? FileText;
  const color = d.pluginInfo?.color ?? "#22c55e";
  const result = d.executionResult;
  const hasError = result?.status === "error";
  const hasSuccess = result?.status === "success";
  const setPreviewNodeId = useFlowStore((s) => s.setPreviewNodeId);
  const previewNodeId = useFlowStore((s) => s.previewNodeId);
  const isPreviewActive = previewNodeId === id;

  return (
    <div
      className={`rounded-xl border-2 bg-zinc-900 shadow-lg min-w-[180px] transition-all ${
        selected ? "ring-2 ring-blue-500" : ""
      }`}
      style={{ borderColor: hasError ? "#ef4444" : hasSuccess ? color : color + "66" }}
    >
      <div
        className="flex items-center gap-2 px-3 py-2 rounded-t-lg text-sm font-semibold"
        style={{ backgroundColor: color + "22", color }}
      >
        <Icon size={16} />
        {d.label ?? d.plugin_id}
      </div>
      {result && (
        <div className="px-3 py-1 text-xs text-zinc-400 border-t border-zinc-800 flex items-center justify-between">
          {hasError ? (
            <span className="text-red-400">{result.error}</span>
          ) : (
            <span>
              {result.rows} rows × {result.columns} cols ({result.duration_ms}ms)
            </span>
          )}
          {hasSuccess && result.preview && result.preview.length > 0 && (
            <button
              onClick={(e) => { e.stopPropagation(); setPreviewNodeId(id); }}
              className={`ml-2 p-0.5 rounded hover:bg-zinc-700 transition-colors ${
                isPreviewActive ? "text-green-400" : "text-zinc-500"
              }`}
              title="Preview data"
            >
              <Eye size={14} />
            </button>
          )}
        </div>
      )}
      <Handle
        type="source"
        position={Position.Right}
        id="output"
        className="!w-3 !h-3 !border-2 !border-zinc-700 !bg-zinc-400"
      />
    </div>
  );
}

export default memo(SourceNode);
