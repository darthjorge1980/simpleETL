import { create } from "zustand";
import {
  type Edge,
  type Node,
  type OnNodesChange,
  type OnEdgesChange,
  type OnConnect,
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
} from "@xyflow/react";
import type {
  NodeData,
  PluginInfo,
  NodeExecutionResult,
} from "@/types";
import { fetchPlugins } from "@/api/client";

let idCounter = 0;
const nextId = () => `node_${++idCounter}`;

interface FlowState {
  nodes: Node<NodeData>[];
  edges: Edge[];
  plugins: PluginInfo[];
  selectedNodeId: string | null;
  executionResults: Record<string, NodeExecutionResult>;

  // actions
  loadPlugins: () => Promise<void>;
  onNodesChange: OnNodesChange;
  onEdgesChange: OnEdgesChange;
  onConnect: OnConnect;
  addNode: (plugin: PluginInfo, position: { x: number; y: number }) => void;
  updateNodeParams: (nodeId: string, params: Record<string, unknown>) => void;
  selectNode: (nodeId: string | null) => void;
  setExecutionResults: (results: Record<string, NodeExecutionResult>) => void;
  deleteSelected: () => void;
  clearFlow: () => void;
  setNodesAndEdges: (nodes: Node<NodeData>[], edges: Edge[]) => void;
  previewNodeId: string | null;
  setPreviewNodeId: (nodeId: string | null) => void;
}

export const useFlowStore = create<FlowState>((set, get) => ({
  nodes: [],
  edges: [],
  plugins: [],
  selectedNodeId: null,
  executionResults: {},
  previewNodeId: null,

  loadPlugins: async () => {
    const plugins = await fetchPlugins();
    set({ plugins });
  },

  onNodesChange: (changes) => {
    set({ nodes: applyNodeChanges(changes, get().nodes) as Node<NodeData>[] });
  },

  onEdgesChange: (changes) => {
    set({ edges: applyEdgeChanges(changes, get().edges) });
  },

  onConnect: (connection) => {
    set({ edges: addEdge(connection, get().edges) });
  },

  addNode: (plugin, position) => {
    const id = nextId();
    const defaultParams: Record<string, unknown> = {};
    for (const p of plugin.params) {
      defaultParams[p.name] = p.default ?? "";
    }

    const newNode: Node<NodeData> = {
      id,
      type: plugin.category,
      position,
      data: {
        plugin_id: plugin.id,
        params: defaultParams,
        label: plugin.name,
        pluginInfo: plugin,
      },
    };
    set({ nodes: [...get().nodes, newNode] });
  },

  updateNodeParams: (nodeId, params) => {
    set({
      nodes: get().nodes.map((n) =>
        n.id === nodeId ? { ...n, data: { ...n.data, params } } : n
      ),
    });
  },

  selectNode: (nodeId) => set({ selectedNodeId: nodeId }),

  setExecutionResults: (results) => {
    set({
      executionResults: results,
      nodes: get().nodes.map((n) => ({
        ...n,
        data: {
          ...n.data,
          executionResult: results[n.id],
        },
      })),
    });
  },

  deleteSelected: () => {
    const { selectedNodeId, nodes, edges } = get();
    if (!selectedNodeId) return;
    set({
      nodes: nodes.filter((n) => n.id !== selectedNodeId),
      edges: edges.filter(
        (e) => e.source !== selectedNodeId && e.target !== selectedNodeId
      ),
      selectedNodeId: null,
    });
  },

  clearFlow: () => set({ nodes: [], edges: [], executionResults: {}, selectedNodeId: null, previewNodeId: null }),

  setNodesAndEdges: (nodes, edges) => set({ nodes, edges }),

  setPreviewNodeId: (nodeId) => set((s) => ({ previewNodeId: s.previewNodeId === nodeId ? null : nodeId })),
}));
