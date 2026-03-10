"""SimpleETL – FastAPI backend."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from core.models import CodeGenRequest, Pipeline, PipelineExecutionResult
from core.plugin_manager import discover_plugins, list_plugins
from core.engine import execute_pipeline
from core.codegen import generate_python

# ── Discover all plugins at startup ─────────────────────────────────
discover_plugins()

# ── App ──────────────────────────────────────────────────────────────
app = FastAPI(title="SimpleETL", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── API routes ───────────────────────────────────────────────────────

@app.get("/api/plugins")
def get_plugins():
    """Return all registered plugins with their metadata."""
    return list_plugins()


@app.post("/api/execute", response_model=PipelineExecutionResult)
def run_pipeline(pipeline: Pipeline):
    """Execute a pipeline and return results with preview data."""
    try:
        return execute_pipeline(pipeline)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/generate")
def generate_code(req: CodeGenRequest):
    """Generate a standalone script from the pipeline."""
    if req.language != "python":
        raise HTTPException(status_code=400, detail="Only Python code generation is supported currently")
    code = generate_python(req.pipeline)
    return {"language": "python", "code": code}


PIPELINES_DIR = Path("pipelines")
PIPELINES_DIR.mkdir(exist_ok=True)


@app.post("/api/pipelines/{name}")
def save_pipeline(name: str, pipeline: Pipeline):
    """Persist a pipeline definition to disk."""
    safe_name = "".join(c for c in name if c.isalnum() or c in "-_")
    if not safe_name:
        raise HTTPException(status_code=400, detail="Invalid pipeline name")
    with open(PIPELINES_DIR / f"{safe_name}.json", "w") as f:
        f.write(pipeline.model_dump_json(indent=2))
    return {"status": "saved", "name": safe_name}


@app.get("/api/pipelines")
def list_pipelines():
    """List saved pipeline names."""
    return [p.stem for p in PIPELINES_DIR.glob("*.json")]


@app.get("/api/pipelines/{name}")
def load_pipeline(name: str):
    """Load a saved pipeline."""
    safe_name = "".join(c for c in name if c.isalnum() or c in "-_")
    path = PIPELINES_DIR / f"{safe_name}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return json.loads(path.read_text())


# ── Serve frontend static files (after API routes) ──────────────────
FRONTEND_DIST = Path(__file__).parent / "frontend_dist"
if FRONTEND_DIST.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIST), html=True), name="static")
