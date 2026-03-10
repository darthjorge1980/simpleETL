import { useFlowStore } from "@/stores/flowStore";
import type { PluginInfo, PluginCategory } from "@/types";
import {
  FileText,
  Braces,
  Table,
  Database,
  Globe,
  HardDrive,
  Filter,
  Columns3,
  PencilLine,
  Merge,
  BarChart3,
  ArrowUpDown,
  Plus,
  Copy,
  FileDown,
  Wrench,
  type LucideIcon,
} from "lucide-react";

const iconMap: Record<string, LucideIcon> = {
  FileText,
  Braces,
  Table,
  Database,
  Globe,
  HardDrive,
  Filter,
  Columns3,
  PencilLine,
  Merge,
  BarChart3,
  ArrowUpDown,
  Plus,
  Copy,
  FileDown,
  Wrench,
};

const categoryLabels: Record<PluginCategory, string> = {
  source: "Sources",
  transform: "Transforms",
  destination: "Destinations",
};

const categoryOrder: PluginCategory[] = ["source", "transform", "destination"];

export default function Sidebar() {
  const plugins = useFlowStore((s) => s.plugins);
  const addNode = useFlowStore((s) => s.addNode);

  const byCategory = categoryOrder.map((cat) => ({
    category: cat,
    label: categoryLabels[cat],
    items: plugins.filter((p) => p.category === cat),
  }));

  const onDragStart = (e: React.DragEvent, plugin: PluginInfo) => {
    e.dataTransfer.setData("application/simpleETL-plugin", JSON.stringify(plugin));
    e.dataTransfer.effectAllowed = "move";
  };

  return (
    <aside className="w-60 bg-zinc-900 border-r border-zinc-800 flex flex-col overflow-hidden">
      <div className="p-4 border-b border-zinc-800">
        <h1 className="text-lg font-bold text-zinc-100 flex items-center gap-2">
          <span className="text-xl">⚡</span> SimpleETL
        </h1>
        <p className="text-xs text-zinc-500 mt-1">Drag nodes to the canvas</p>
      </div>
      <div className="flex-1 overflow-y-auto p-3 space-y-4">
        {byCategory.map(({ category, label, items }) => (
          <div key={category}>
            <h2 className="text-xs font-semibold text-zinc-500 uppercase tracking-wider mb-2">
              {label}
            </h2>
            <div className="space-y-1">
              {items.map((plugin) => {
                const Icon = iconMap[plugin.icon] ?? Wrench;
                return (
                  <div
                    key={plugin.id}
                    draggable
                    onDragStart={(e) => onDragStart(e, plugin)}
                    onClick={() =>
                      addNode(plugin, {
                        x: 250 + Math.random() * 200,
                        y: 100 + Math.random() * 300,
                      })
                    }
                    className="flex items-center gap-2 px-3 py-2 rounded-lg cursor-grab active:cursor-grabbing hover:bg-zinc-800 transition-colors text-sm"
                    title={plugin.description}
                  >
                    <Icon size={14} />
                    <span>{plugin.name}</span>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </aside>
  );
}
