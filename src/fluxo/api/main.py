from fastapi import FastAPI, HTTPException, Depends
from typing import List, Dict, Any
from pathlib import Path
import os
import functools

from fluxo.core.parser import parse_manifest
from fluxo.core.runner import StateManager
from fluxo.core.graph import FluxoGraph

app = FastAPI(title="Fluxo API")

@functools.lru_cache(maxsize=1)
def get_project_context():
    # In a real app, this might be configurable. For now, we assume local.
    manifest_path = "fluxo.yaml"
    if not os.path.exists(manifest_path):
         # Try to look in parent or example if needed, but usually current dir
         pass
    
    try:
        manifest = parse_manifest(manifest_path)
        graph = FluxoGraph(manifest)
        state_manager = StateManager()
        return manifest, graph, state_manager
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading Fluxo project: {str(e)}")

@app.get("/status")
def get_status(context = Depends(get_project_context)):
    manifest, graph, state_manager = context
    states = state_manager.get_all_states()
    
    results = []
    for task in manifest.tasks:
        s = states.get(task.name, {"status": "Pending", "updated_at": None, "error_message": ""})
        results.append({
            "name": task.name,
            "status": s.get("status", "Pending"),
            "last_updated": s.get("updated_at"),
            "error": s.get("error_message", ""),
            "row_count": s.get("row_count", 0),
            "duration": s.get("duration", 0.0),
            "qc_results": s.get("qc_results", "[]"),
            "sql_path": task.source_sql
        })
    return results

@app.get("/lineage")
def get_lineage(context = Depends(get_project_context)):
    manifest, graph, state_manager = context
    states = state_manager.get_all_states()
    
    nodes = []
    edges = []
    
    # Colors mapping
    COLORS = {
        "Pending": "#808080",
        "Running": "#3498db",  # Blue
        "Success": "#2ecc71",  # Green
        "Failed": "#e74c3c"    # Red
    }
    
    for task in manifest.tasks:
        status = states.get(task.name, {}).get("status", "Pending")
        nodes.append({
            "id": task.name,
            "label": task.name,
            "color": COLORS.get(status, "#808080"),
            "status": status,
            "duration": states.get(task.name, {}).get("duration", 0.0),
            "row_count": states.get(task.name, {}).get("row_count", 0),
            "qc_results": states.get(task.name, {}).get("qc_results", "[]"),
            "sql": task.sql_content or ""
        })
        
        for dep in task.depends_on:
            edges.append({
                "from": dep,
                "to": task.name
            })
            
    return {"nodes": nodes, "edges": edges}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
