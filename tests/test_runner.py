import pytest # type: ignore
from fluxo.core.runner import FluxoRunner, StateManager # type: ignore
from fluxo.core.parser import parse_manifest # type: ignore
from fluxo.core.adapters import BaseAdapter # type: ignore
from typing import Tuple, List, Dict, Any
import json
import os

class MockAdapter(BaseAdapter):
    def __init__(self, fail_count=0):
        self.executed_queries = []
        self.fail_count = fail_count
        self.attempts = 0

    def execute(self, query: str) -> None:
        self.attempts += 1
        if self.attempts <= self.fail_count:
            raise ConnectionError(f"Mock Connection Dropped! Attempt {self.attempts}")
        self.executed_queries.append(query)

    def fetchone(self, query: str) -> Tuple:
        return (1,)

    def fetchall(self, query: str) -> List[Tuple]:
        return [(1,)]

    def get_row_count(self, table_name: str) -> int:
        return 100

    def get_profiling_stats(self, table_name: str, numeric_columns: List[str]) -> Dict[str, Any]:
        return {"id": {"min": 1, "max": 100, "mean": 50.0, "null_count": 0}}

    def close(self):
        pass

def test_runner_retry_logic(tmp_path):
    manifest_yaml = """
profile_target: dev
tasks:
  - name: retry_model
    source_sql: models/retry.sql
"""
    manifest_file = tmp_path / "fluxo.yaml"
    manifest_file.write_text(manifest_yaml)
    
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    sql_file = models_dir / "retry.sql"
    sql_file.write_text("SELECT 1 AS retry_col")
    
    # Needs a mock profiles.yaml so parser doesn't crash if we test the full pipeline
    profiles_yaml = """
dev:
  connection_type: mock
"""
    (tmp_path / "profiles.yaml").write_text(profiles_yaml)
    
    manifest = parse_manifest(str(manifest_file), profiles_path=str(tmp_path/"profiles.yaml"), project_root=tmp_path)
    runner = FluxoRunner(manifest)
    
    # Inject our mock adapter designed to fail 2 times then succeed
    mock_adapter = MockAdapter(fail_count=2)
    runner.adapter = mock_adapter
    # Override the state DB to point to temp dir
    runner.state = StateManager(db_path=str(tmp_path / ".fluxo_test_state.db"))
    
    runner.run_all()
    
    states = runner.state.get_all_states()
    assert states["retry_model"]["status"] == "Success"
    assert mock_adapter.attempts == 3 # 2 failures + 1 success = 3 attempts total

def test_empty_sql_handling(tmp_path):
    manifest_yaml = """
profile_target: dev
tasks:
  - name: empty_model
    source_sql: models/empty.sql
"""
    manifest_file = tmp_path / "fluxo.yaml"
    manifest_file.write_text(manifest_yaml)
    
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "empty.sql").write_text("   \n  ") # Whitespace only
    
    manifest = parse_manifest(str(manifest_file), project_root=tmp_path)
    runner = FluxoRunner(manifest)
    runner.adapter = MockAdapter()
    runner.state = StateManager(db_path=str(tmp_path / ".fluxo_test_state.db"))
    
    runner.run_all()
    
    states = runner.state.get_all_states()
    # Let's ensure empty queries don't crash but act appropriately.
    # Depending on implementation, they might succeed with 0 rows or fail.
    # Currently `execute ""` might pass through adapter successfully or fail on syntax. 
    # With MockAdapter it just appends empty string and passes.
    assert states["empty_model"]["status"] == "Success"
