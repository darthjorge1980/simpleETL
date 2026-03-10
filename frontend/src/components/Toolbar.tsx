import { useState, useCallback } from "react";
import { Play, Code, Save, FolderOpen, Trash2, Loader2 } from "lucide-react";
import { useFlowStore } from "@/stores/flowStore";
import {
  executePipeline,
  generateCode,
  savePipeline,
  listPipelines,
  loadPipeline,
  type PipelinePayload,
} from "@/api/client";

export default function Toolbar() {
  const { nodes, edges, setExecutionResults, clearFlow, setNodesAndEdges } =
    useFlowStore();
  const [running, setRunning] = useState(false);
  const [codeModal, setCodeModal] = useState<string | null>(null);
  const [pipelineListModal, setPipelineListModal] = useState(false);
  const [pipelineNames, setPipelineNames] = useState<string[]>([]);

  const buildPayload = useCallback((): PipelinePayload => {
    return {
      nodes: nodes.map((n) => ({
        id: n.id,
        type: n.type ?? "transform",
        position: { x: n.position.x, y: n.position.y },
        data: {
          plugin_id: n.data.plugin_id,
          params: n.data.params,
        },
      })),
      edges: edges.map((e) => ({
        id: e.id,
        source: e.source,
        target: e.target,
        source_handle: (e.sourceHandle as string) ?? "output",
        target_handle: (e.targetHandle as string) ?? "input",
      })),
    };
  }, [nodes, edges]);

  const handleRun = async () => {
    if (nodes.length === 0) return;
    setRunning(true);
    try {
      const result = await executePipeline(buildPayload());
      setExecutionResults(result.node_results);
    } catch (err) {
      alert("Execution failed: " + (err as Error).message);
    } finally {
      setRunning(false);
    }
  };

  const handleGenerate = async () => {
    try {
      const result = await generateCode(buildPayload());
      setCodeModal(result.code);
    } catch (err) {
      alert("Code generation failed: " + (err as Error).message);
    }
  };

  const handleSave = async () => {
    const name = prompt("Pipeline name:");
    if (!name) return;
    try {
      await savePipeline(name, buildPayload());
      alert("Saved!");
    } catch (err) {
      alert("Save failed: " + (err as Error).message);
    }
  };

  const handleLoad = async () => {
    try {
      const names = await listPipelines();
      setPipelineNames(names);
      setPipelineListModal(true);
    } catch (err) {
      alert("Failed to list pipelines: " + (err as Error).message);
    }
  };

  const handleLoadPipeline = async (name: string) => {
    try {
      const data = await loadPipeline(name);
      const plugins = useFlowStore.getState().plugins;
      const newNodes = data.nodes.map((n) => {
        const plugin = plugins.find((p) => p.id === n.data.plugin_id);
        return {
          id: n.id,
          type: n.type,
          position: n.position,
          data: {
            ...n.data,
            label: plugin?.name ?? n.data.plugin_id,
            pluginInfo: plugin,
          },
        };
      });
      const newEdges = data.edges.map((e) => ({
        id: e.id,
        source: e.source,
        target: e.target,
        sourceHandle: e.source_handle,
        targetHandle: e.target_handle,
      }));
      setNodesAndEdges(newNodes, newEdges);
      setPipelineListModal(false);
    } catch (err) {
      alert("Load failed: " + (err as Error).message);
    }
  };

  return (
    <>
      <div className="h-12 bg-zinc-900 border-b border-zinc-800 flex items-center px-4 gap-2">
        <button
          onClick={handleRun}
          disabled={running || nodes.length === 0}
          className="flex items-center gap-1.5 px-3 py-1.5 bg-green-600 hover:bg-green-700 disabled:opacity-50 text-white text-sm rounded-lg font-medium transition-colors"
        >
          {running ? <Loader2 size={14} className="animate-spin" /> : <Play size={14} />}
          {running ? "Running…" : "Run"}
        </button>

        <button
          onClick={handleGenerate}
          disabled={nodes.length === 0}
          className="flex items-center gap-1.5 px-3 py-1.5 bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 text-white text-sm rounded-lg font-medium transition-colors"
        >
          <Code size={14} />
          Generate Code
        </button>

        <div className="w-px h-6 bg-zinc-700 mx-1" />

        <button
          onClick={handleSave}
          className="flex items-center gap-1.5 px-3 py-1.5 bg-zinc-700 hover:bg-zinc-600 text-white text-sm rounded-lg transition-colors"
        >
          <Save size={14} />
          Save
        </button>

        <button
          onClick={handleLoad}
          className="flex items-center gap-1.5 px-3 py-1.5 bg-zinc-700 hover:bg-zinc-600 text-white text-sm rounded-lg transition-colors"
        >
          <FolderOpen size={14} />
          Load
        </button>

        <div className="flex-1" />

        <button
          onClick={clearFlow}
          className="flex items-center gap-1.5 px-3 py-1.5 text-zinc-400 hover:text-red-400 text-sm rounded-lg transition-colors"
        >
          <Trash2 size={14} />
          Clear
        </button>
      </div>

      {/* Code modal */}
      {codeModal !== null && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
          <div className="bg-zinc-900 border border-zinc-700 rounded-xl w-[700px] max-h-[80vh] flex flex-col shadow-2xl">
            <div className="flex items-center justify-between px-5 py-3 border-b border-zinc-800">
              <h3 className="font-semibold text-zinc-100">Generated Python Script</h3>
              <button
                onClick={() => setCodeModal(null)}
                className="text-zinc-400 hover:text-zinc-100"
              >
                ✕
              </button>
            </div>
            <pre className="p-5 flex-1 overflow-auto text-sm text-zinc-300 font-mono whitespace-pre-wrap">
              {codeModal}
            </pre>
            <div className="px-5 py-3 border-t border-zinc-800 flex gap-2">
              <button
                onClick={() => {
                  navigator.clipboard.writeText(codeModal);
                }}
                className="px-3 py-1.5 bg-indigo-600 hover:bg-indigo-700 text-white text-sm rounded-lg"
              >
                Copy to Clipboard
              </button>
              <button
                onClick={() => {
                  const blob = new Blob([codeModal], { type: "text/x-python" });
                  const url = URL.createObjectURL(blob);
                  const a = document.createElement("a");
                  a.href = url;
                  a.download = "etl_pipeline.py";
                  a.click();
                  URL.revokeObjectURL(url);
                }}
                className="px-3 py-1.5 bg-zinc-700 hover:bg-zinc-600 text-white text-sm rounded-lg"
              >
                Download .py
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Pipeline list modal */}
      {pipelineListModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
          <div className="bg-zinc-900 border border-zinc-700 rounded-xl w-[400px] shadow-2xl">
            <div className="flex items-center justify-between px-5 py-3 border-b border-zinc-800">
              <h3 className="font-semibold text-zinc-100">Load Pipeline</h3>
              <button
                onClick={() => setPipelineListModal(false)}
                className="text-zinc-400 hover:text-zinc-100"
              >
                ✕
              </button>
            </div>
            <div className="p-4 max-h-[400px] overflow-y-auto space-y-1">
              {pipelineNames.length === 0 && (
                <p className="text-zinc-500 text-sm">No saved pipelines.</p>
              )}
              {pipelineNames.map((name) => (
                <button
                  key={name}
                  onClick={() => handleLoadPipeline(name)}
                  className="w-full text-left px-3 py-2 rounded-lg hover:bg-zinc-800 text-sm text-zinc-300"
                >
                  {name}
                </button>
              ))}
            </div>
          </div>
        </div>
      )}
    </>
  );
}
