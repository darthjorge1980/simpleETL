import { memo } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import type { NodeData } from "@/types";
import { FileDown, HardDrive, type LucideIcon } from "lucide-react";

const iconMap: Record<string, LucideIcon> = {
  FileDown,
  HardDrive,
};

function DestinationNode({ data, selected }: NodeProps) {
  const d = data as unknown as NodeData;
  const Icon = iconMap[d.pluginInfo?.icon ?? ""] ?? FileDown;
  const color = d.pluginInfo?.color ?? "#f59e0b";
  const result = d.executionResult;
  const hasError = result?.status === "error";
  const hasSuccess = result?.status === "success";

  return (
    <div
      className={`rounded-xl border-2 bg-zinc-900 shadow-lg min-w-[180px] transition-all ${
        selected ? "ring-2 ring-blue-500" : ""
      }`}
      style={{ borderColor: hasError ? "#ef4444" : hasSuccess ? color : color + "66" }}
    >
      <Handle
        type="target"
        position={Position.Left}
        id="input"
        className="!w-3 !h-3 !border-2 !border-zinc-700 !bg-zinc-400"
      />
      <div
        className="flex items-center gap-2 px-3 py-2 rounded-t-lg text-sm font-semibold"
        style={{ backgroundColor: color + "22", color }}
      >
        <Icon size={16} />
        {d.label ?? d.plugin_id}
      </div>
      {result && (
        <div className="px-3 py-1 text-xs text-zinc-400 border-t border-zinc-800">
          {hasError ? (
            <span className="text-red-400">{result.error}</span>
          ) : (
            <span className="text-green-400">Written ({result.duration_ms}ms)</span>
          )}
        </div>
      )}
    </div>
  );
}

export default memo(DestinationNode);
