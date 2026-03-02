import pytest # type: ignore
from fastapi.testclient import TestClient # type: ignore
from fluxo.api.main import app, get_project_context # type: ignore
import os

client = TestClient(app)

# We use the dependency override to avoid relying on a real project directory for the base tests
def override_get_project_context():
    from fluxo.core.parser import FluxoManifest, FluxoTask # type: ignore
    from fluxo.api.main import get_project_context # type: ignore
    from fluxo.core.graph import FluxoGraph # type: ignore
    from fluxo.core.runner import StateManager # type: ignore
    
    get_project_context.cache_clear()
    
    task_a = FluxoTask(name="model_a", source_sql="models/a.sql", materialized="table", depends_on=[])
    task_b = FluxoTask(name="model_b", source_sql="models/b.sql", materialized="view", depends_on=["model_a"])
    
    manifest = FluxoManifest(profile_target="dev", tasks=[task_a, task_b])
    graph = FluxoGraph(manifest)
    state = StateManager(db_path=":memory:")
    
    return manifest, graph, state

app.dependency_overrides[get_project_context] = override_get_project_context

def test_status_endpoint():
    response = client.get("/status")
    assert response.status_code == 200
    
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 2
    assert data[0]["name"] == "model_a"

def test_lineage_endpoint():
    response = client.get("/lineage")
    assert response.status_code == 200
    
    data = response.json()
    assert "nodes" in data
    assert "edges" in data
    
    nodes = {n["id"] for n in data["nodes"]}
    assert "model_a" in nodes
    assert "model_b" in nodes
    
    # Check edges
    edges = data["edges"]
    assert len(edges) == 1
    assert edges[0]["source"] == "model_a"
    assert edges[0]["target"] == "model_b"
