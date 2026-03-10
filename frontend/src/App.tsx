import { useEffect } from "react";
import Sidebar from "./components/Sidebar";
import Toolbar from "./components/Toolbar";
import FlowEditor from "./components/FlowEditor";
import PropertiesPanel from "./components/PropertiesPanel";
import DataPreview from "./components/DataPreview";
import { useFlowStore } from "./stores/flowStore";

export default function App() {
  const loadPlugins = useFlowStore((s) => s.loadPlugins);

  useEffect(() => {
    loadPlugins();
  }, [loadPlugins]);

  return (
    <div className="h-screen w-screen flex flex-col overflow-hidden">
      <Toolbar />
      <div className="flex flex-1 overflow-hidden relative">
        <Sidebar />
        <FlowEditor />
        <PropertiesPanel />
        <DataPreview />
      </div>
    </div>
  );
}
