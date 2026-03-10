import { memo } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import type { NodeData } from "@/types";
import { useFlowStore } from "@/stores/flowStore";
import {
  Filter,
  Columns3,
  PencilLine,
  Merge,
  BarChart3,
  ArrowUpDown,
  Plus,
  Copy,
  Wrench,
  Eye,
  Layers,
  Search,
  RotateCw,
  FlipVertical,
  Key,
  ListFilter,
  Eraser,
  Binary,
  Type,
  Calendar,
  Hash,
  Rows3,
  Scissors,
  Combine,
  Expand,
  GitBranch,
  GitCompare,
  Split,
  type LucideIcon,
} from "lucide-react";

const iconMap: Record<string, LucideIcon> = {
  Filter,
  Columns3,
  PencilLine,
  Merge,
  BarChart3,
  ArrowUpDown,
  Plus,
  Copy,
  Wrench,
  Eye,
  Layers,
  Search,
  RotateCw,
  FlipVertical,
  Key,
  ListFilter,
  Eraser,
  Binary,
  Type,
  Calendar,
  Hash,
  Rows3,
  Scissors,
  Combine,
  Expand,
  GitBranch,
  GitCompare,
  Split,
};

function TransformNode({ id, data, selected }: NodeProps) {
  const d = data as unknown as NodeData;
  const Icon = iconMap[d.pluginInfo?.icon ?? ""] ?? Wrench;
  const color = d.pluginInfo?.color ?? "#3b82f6";
  const result = d.executionResult;
  const hasError = result?.status === "error";
  const hasSuccess = result?.status === "success";
  const setPreviewNodeId = useFlowStore((s) => s.setPreviewNodeId);
  const previewNodeId = useFlowStore((s) => s.previewNodeId);
  const isPreviewActive = previewNodeId === id;
  const isPreviewNode = d.plugin_id === "data_preview";

  const inputs = d.pluginInfo?.inputs ?? [{ name: "input" }];
  const outputs = d.pluginInfo?.outputs ?? [{ name: "output" }];

  const previewRows = result?.preview?.slice(0, Number(d.params?.max_rows ?? 5)) ?? [];
  const previewCols = previewRows.length > 0 ? Object.keys(previewRows[0]) : [];

  return (
    <div
      className={`rounded-xl border-2 bg-zinc-900 shadow-lg transition-all ${
        isPreviewNode ? "min-w-[280px] max-w-[480px]" : "min-w-[180px]"
      } ${selected ? "ring-2 ring-blue-500" : ""}`}
      style={{ borderColor: hasError ? "#ef4444" : hasSuccess ? color : color + "66" }}
    >
      {inputs.map((inp, idx) => (
        <Handle
          key={inp.name}
          type="target"
          position={Position.Left}
          id={inp.name}
          className="!w-3 !h-3 !border-2 !border-zinc-700 !bg-zinc-400"
          style={inputs.length > 1 ? { top: `${((idx + 1) / (inputs.length + 1)) * 100}%` } : undefined}
        >
          {inputs.length > 1 && (
            <span className="absolute left-4 -top-2 text-[9px] text-zinc-500 pointer-events-none select-none whitespace-nowrap">
              {inp.name}
            </span>
          )}
        </Handle>
      ))}
      <div
        className="flex items-center gap-2 px-3 py-2 rounded-t-lg text-sm font-semibold"
        style={{ backgroundColor: color + "22", color }}
      >
        <Icon size={16} />
        {d.label ?? d.plugin_id}
      </div>

      {/* Inline data table for Preview Data nodes */}
      {isPreviewNode && hasSuccess && previewRows.length > 0 && (
        <div className="border-t border-zinc-800 max-h-[200px] overflow-auto nowheel">
          <table className="text-[10px] border-collapse">
            <thead className="sticky top-0 z-10">
              <tr className="bg-zinc-800">
                <th className="px-1.5 py-1 text-zinc-500 font-medium border-r border-zinc-700 text-left">#</th>
                {previewCols.map((col) => (
                  <th key={col} className="px-1.5 py-1 text-zinc-400 font-medium border-r border-zinc-700 text-left whitespace-nowrap">
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {previewRows.map((row, i) => (
                <tr key={i} className="hover:bg-zinc-800/50">
                  <td className="px-1.5 py-0.5 text-zinc-600 border-r border-zinc-800/50 font-mono">{i + 1}</td>
                  {previewCols.map((col) => (
                    <td key={col} className="px-1.5 py-0.5 text-zinc-300 border-r border-zinc-800/50 whitespace-nowrap max-w-[150px] truncate" title={String(row[col] ?? "")}>
                      {row[col] === null ? <span className="text-zinc-600 italic">null</span> : String(row[col])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {isPreviewNode && !hasSuccess && !hasError && (
        <div className="px-3 py-3 text-xs text-zinc-600 border-t border-zinc-800 text-center italic">
          Run pipeline to see data
        </div>
      )}

      {result && (
        <div className="px-3 py-1 text-xs text-zinc-400 border-t border-zinc-800 flex items-center justify-between">
          {hasError ? (
            <span className="text-red-400">{result.error}</span>
          ) : (
            <span>
              {result.rows} rows × {result.columns} cols ({result.duration_ms}ms)
            </span>
          )}
          {hasSuccess && result.preview && result.preview.length > 0 && !isPreviewNode && (
            <button
              onClick={(e) => { e.stopPropagation(); setPreviewNodeId(id); }}
              className={`ml-2 p-0.5 rounded hover:bg-zinc-700 transition-colors ${
                isPreviewActive ? "text-blue-400" : "text-zinc-500"
              }`}
              title="Preview data"
            >
              <Eye size={14} />
            </button>
          )}
        </div>
      )}
      {outputs.map((out, idx) => (
        <Handle
          key={out.name}
          type="source"
          position={Position.Right}
          id={out.name}
          className="!w-3 !h-3 !border-2 !border-zinc-700 !bg-zinc-400"
          style={outputs.length > 1 ? { top: `${((idx + 1) / (outputs.length + 1)) * 100}%` } : undefined}
        >
          {outputs.length > 1 && (
            <span className="absolute right-4 -top-2 text-[9px] text-zinc-500 pointer-events-none select-none whitespace-nowrap">
              {out.name}
            </span>
          )}
        </Handle>
      ))}
    </div>
  );
}

export default memo(TransformNode);
