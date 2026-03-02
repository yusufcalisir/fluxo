import pytest # type: ignore
from fluxo.core.parser import parse_manifest, FluxoManifest # type: ignore
from fluxo.core.graph import FluxoGraph # type: ignore
import os
import tempfile
from pathlib import Path

def test_parse_valid_manifest(tmp_path):
    manifest_yaml = """
profile_target: dev
tasks:
  - name: model_a
    source_sql: models/a.sql
  - name: model_b
    source_sql: models/b.sql
    depends_on: [model_a]
"""
    manifest_file = tmp_path / "fluxo.yaml"
    manifest_file.write_text(manifest_yaml)
    
    # We create empty SQL files so the 'exists' check passes in the parser's auto-detect logic
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "a.sql").write_text("SELECT 1 AS id")
    (models_dir / "b.sql").write_text("SELECT 2 AS id FROM model_a")

    manifest = parse_manifest(str(manifest_file), project_root=tmp_path)
    assert len(manifest.tasks) == 2
    assert manifest.tasks[0].name == "model_a"
    assert manifest.tasks[1].depends_on == ["model_a"]

def test_parse_invalid_yaml(tmp_path):
    manifest_yaml = """
profile_target: dev
tasks:
  - name: model_a
   source_sql: indentation_error
"""
    manifest_file = tmp_path / "fluxo.yaml"
    manifest_file.write_text(manifest_yaml)

    with pytest.raises(ValueError, match="Error parsing YAML"):
        parse_manifest(str(manifest_file), project_root=tmp_path)

def test_missing_manifest():
    with pytest.raises(FileNotFoundError):
        parse_manifest("doesnt_exist.yaml")

def test_circular_dependency(tmp_path):
    manifest_yaml = """
tasks:
  - name: model_a
    source_sql: models/a.sql
    depends_on: [model_b]
  - name: model_b
    source_sql: models/b.sql
    depends_on: [model_a]
"""
    manifest_file = tmp_path / "fluxo.yaml"
    manifest_file.write_text(manifest_yaml)
    
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "a.sql").write_text("SELECT * FROM model_b")
    (models_dir / "b.sql").write_text("SELECT * FROM model_a")

    manifest = parse_manifest(str(manifest_file), project_root=tmp_path)
    
    with pytest.raises(ValueError, match="Circular dependency detected"):
        FluxoGraph(manifest)
