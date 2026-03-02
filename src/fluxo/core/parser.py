import yaml # type: ignore
import sqlglot # type: ignore
from sqlglot import exp # type: ignore
from pathlib import Path
from typing import List, Optional, Set, Dict, Any
from pydantic import BaseModel, Field, ValidationError # type: ignore
import os
from dotenv import load_dotenv # type: ignore

load_dotenv()

class TaskTests(BaseModel):
    not_null: List[str] = Field(default_factory=list)
    unique: List[str] = Field(default_factory=list)
    accepted_values: Dict[str, List[Any]] = Field(default_factory=dict)
    row_count_min: Optional[int] = None

class FluxoTask(BaseModel):
    name: str
    source_sql: str
    materialized: str = Field(default="table")
    unique_key: Optional[str] = None
    timestamp_col: Optional[str] = None
    depends_on: List[str] = Field(default_factory=list)
    sql_content: Optional[str] = None
    tests: TaskTests = Field(default_factory=TaskTests)

class FluxoManifest(BaseModel):
    tasks: List[FluxoTask]
    webhook_url: Optional[str] = None
    profile_target: str = Field(default="dev")
    adapter_config: Dict[str, Any] = Field(default_factory=dict)
    project_root: Optional[str] = None

def extract_dependencies_from_sql(sql: str) -> Set[str]:
    """
    Extracts table dependencies from SQL using sqlglot.
    Looks for tables in FROM and JOIN clauses.
    """
    try:
        # Parse the SQL and find all Table expressions
        tables = set()
        for expression in sqlglot.parse_one(sql).find_all(exp.Table):
            # We assume table names match task names in this simple version
            tables.add(expression.name)
        return tables
    except Exception:
        # Fallback to empty if parsing fails
        return set()

def parse_manifest(manifest_path: str = "fluxo.yaml", profiles_path: str = "profiles.yaml", project_root: Optional[Path] = None) -> FluxoManifest:
    """
    Reads and validates the fluxo.yaml manifest file.
    Also merges adapter config from profiles.yaml if it exists.
    Auto-detects dependencies from SQL files.
    """
    path = Path(manifest_path)
    if not path.exists():
        raise FileNotFoundError(f"Manifest file not found: {manifest_path}")

    root = project_root or path.parent

    with open(path, "r", encoding="utf-8") as f:
        try:
            data = yaml.safe_load(f)
            if data is None:
                data = {"tasks": []}
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML from {manifest_path}: {e}")

    try:
        manifest = FluxoManifest(**data)
        manifest.project_root = str(root)
        
        # Load Profiles
        prof_path = Path(profiles_path)
        if prof_path.exists():
            with open(prof_path, "r", encoding="utf-8") as pf:
                # Expand environment variables like ${DB_PASSWORD} securely
                expanded_yaml = os.path.expandvars(pf.read())
                profiles_data = yaml.safe_load(expanded_yaml) or {}
                # Assuming simple structure for now:
                # { "dev": { "connection_type": "duckdb", "db_path": "..." } }
                target_config = profiles_data.get(manifest.profile_target, {})
                manifest.adapter_config = target_config

        # Enhance tasks with auto-detected dependencies
        for task in manifest.tasks:
            sql_file = root / task.source_sql
            if sql_file.exists():
                sql_text = sql_file.read_text(encoding="utf-8")
                task.sql_content = sql_text
                
                auto_deps = extract_dependencies_from_sql(sql_text)
                
                # Merge manual and auto-detected dependencies
                existing_deps = set(task.depends_on)
                # Filter out itself if accidentally referenced
                auto_deps.discard(task.name)
                
                # We only want to add dependencies that actually exist as tasks
                all_task_names = {t.name for t in manifest.tasks}
                valid_auto_deps = {dep for dep in auto_deps if dep in all_task_names}
                
                task.depends_on = list(existing_deps.union(valid_auto_deps))
                
        return manifest
    except ValidationError as e:
        raise ValueError(f"Invalid manifest structure in {manifest_path}:\n{e}")
