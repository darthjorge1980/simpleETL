import { X } from "lucide-react";
import { useFlowStore } from "@/stores/flowStore";

export default function DataPreview() {
  const previewNodeId = useFlowStore((s) => s.previewNodeId);
  const setPreviewNodeId = useFlowStore((s) => s.setPreviewNodeId);
  const nodes = useFlowStore((s) => s.nodes);
  const executionResults = useFlowStore((s) => s.executionResults);

  if (!previewNodeId) return null;

  const node = nodes.find((n) => n.id === previewNodeId);
  const result = executionResults[previewNodeId];
  if (!node || !result?.preview?.length) return null;

  const label = node.data.pluginInfo?.name ?? node.data.plugin_id;
  const columns = Object.keys(result.preview[0]);
  const rows = result.preview.slice(0, 5);

  return (
    <div className="absolute bottom-0 left-0 right-0 z-50 bg-zinc-900 border-t-2 border-zinc-700 shadow-2xl max-h-[260px] flex flex-col">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-zinc-800 shrink-0">
        <div className="flex items-center gap-3">
          <span className="text-sm font-semibold text-zinc-200">
            Data Preview
          </span>
          <span className="text-xs text-zinc-500">
            {label} — {result.rows} rows × {result.columns} cols — showing {rows.length}
          </span>
        </div>
        <button
          onClick={() => setPreviewNodeId(previewNodeId)}
          className="p-1 rounded hover:bg-zinc-700 text-zinc-400 hover:text-zinc-200 transition-colors"
          title="Close preview"
        >
          <X size={16} />
        </button>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto">
        <table className="w-full text-xs border-collapse">
          <thead className="sticky top-0 z-10">
            <tr className="bg-zinc-800">
              <th className="px-3 py-1.5 text-left text-zinc-500 font-medium border-r border-zinc-700 w-8">
                #
              </th>
              {columns.map((col) => (
                <th
                  key={col}
                  className="px-3 py-1.5 text-left text-zinc-400 font-medium border-r border-zinc-700 whitespace-nowrap"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr
                key={i}
                className="border-b border-zinc-800/50 hover:bg-zinc-800/70 transition-colors"
              >
                <td className="px-3 py-1.5 text-zinc-600 border-r border-zinc-800/50 font-mono">
                  {i + 1}
                </td>
                {columns.map((col) => (
                  <td
                    key={col}
                    className="px-3 py-1.5 text-zinc-300 border-r border-zinc-800/50 whitespace-nowrap max-w-[300px] truncate"
                    title={String(row[col] ?? "")}
                  >
                    {row[col] === null ? (
                      <span className="text-zinc-600 italic">null</span>
                    ) : (
                      String(row[col])
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
