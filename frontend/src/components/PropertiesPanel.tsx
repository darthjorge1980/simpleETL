import { useFlowStore } from "@/stores/flowStore";
import type { ParamDefinition } from "@/types";

export default function PropertiesPanel() {
  const selectedNodeId = useFlowStore((s) => s.selectedNodeId);
  const nodes = useFlowStore((s) => s.nodes);
  const updateNodeParams = useFlowStore((s) => s.updateNodeParams);
  const deleteSelected = useFlowStore((s) => s.deleteSelected);
  const executionResults = useFlowStore((s) => s.executionResults);

  const node = nodes.find((n) => n.id === selectedNodeId);
  if (!node) {
    return (
      <aside className="w-72 bg-zinc-900 border-l border-zinc-800 flex items-center justify-center">
        <p className="text-zinc-600 text-sm">Select a node to edit</p>
      </aside>
    );
  }

  const pluginInfo = node.data.pluginInfo;
  const params = node.data.params;
  const result = executionResults[node.id];

  const onParamChange = (paramName: string, value: unknown) => {
    updateNodeParams(node.id, { ...params, [paramName]: value });
  };

  const renderField = (p: ParamDefinition) => {
    const val = params[p.name] ?? p.default ?? "";

    switch (p.param_type) {
      case "boolean":
        return (
          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              checked={!!val}
              onChange={(e) => onParamChange(p.name, e.target.checked)}
              className="accent-indigo-500"
            />
            <span className="text-sm text-zinc-300">{val ? "Yes" : "No"}</span>
          </label>
        );

      case "select":
        return (
          <select
            value={String(val)}
            onChange={(e) => onParamChange(p.name, e.target.value)}
            className="w-full px-2 py-1.5 bg-zinc-800 border border-zinc-700 rounded-lg text-sm text-zinc-200 focus:outline-none focus:border-indigo-500"
          >
            {(p.options ?? []).map((opt) => (
              <option key={opt} value={opt}>
                {opt}
              </option>
            ))}
          </select>
        );

      case "number":
        return (
          <input
            type="number"
            value={val as number}
            onChange={(e) => onParamChange(p.name, Number(e.target.value))}
            className="w-full px-2 py-1.5 bg-zinc-800 border border-zinc-700 rounded-lg text-sm text-zinc-200 focus:outline-none focus:border-indigo-500"
          />
        );

      case "code":
        return (
          <textarea
            value={String(val)}
            onChange={(e) => onParamChange(p.name, e.target.value)}
            rows={4}
            className="w-full px-2 py-1.5 bg-zinc-800 border border-zinc-700 rounded-lg text-sm text-zinc-200 font-mono focus:outline-none focus:border-indigo-500"
          />
        );

      default:
        return (
          <input
            type="text"
            value={String(val)}
            onChange={(e) => onParamChange(p.name, e.target.value)}
            className="w-full px-2 py-1.5 bg-zinc-800 border border-zinc-700 rounded-lg text-sm text-zinc-200 focus:outline-none focus:border-indigo-500"
          />
        );
    }
  };

  return (
    <aside className="w-72 bg-zinc-900 border-l border-zinc-800 flex flex-col overflow-hidden">
      <div className="p-4 border-b border-zinc-800">
        <h2 className="font-semibold text-zinc-100 text-sm">
          {pluginInfo?.name ?? node.data.plugin_id}
        </h2>
        {pluginInfo?.description && (
          <p className="text-xs text-zinc-500 mt-1">{pluginInfo.description}</p>
        )}
      </div>

      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {(pluginInfo?.params ?? []).map((p) => (
          <div key={p.name}>
            <label className="block text-xs font-medium text-zinc-400 mb-1">
              {p.label}
              {p.required && <span className="text-red-400 ml-0.5">*</span>}
            </label>
            {renderField(p)}
            {p.description && (
              <p className="text-xs text-zinc-600 mt-0.5">{p.description}</p>
            )}
          </div>
        ))}
      </div>

      <div className="p-4 border-t border-zinc-800">
        <button
          onClick={deleteSelected}
          className="w-full py-1.5 text-red-400 hover:bg-red-400/10 text-sm rounded-lg transition-colors"
        >
          Delete Node
        </button>
      </div>
    </aside>
  );
}
