import duckdb # type: ignore
import sqlite3
import json
from pathlib import Path
from typing import List
from rich.console import Console # type: ignore
from rich.panel import Panel # type: ignore

from fluxo.core.parser import FluxoManifest # type: ignore
from fluxo.core.graph import FluxoGraph # type: ignore

console = Console()

class StateManager:
    """Manages the execution state of tasks using SQLite."""
    def __init__(self, db_path: str = ".fluxo_state.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS task_state (
                    task_name TEXT PRIMARY KEY,
                    status TEXT,
                    error_message TEXT,
                    row_count INTEGER DEFAULT 0,
                    duration REAL DEFAULT 0.0,
                    qc_results TEXT DEFAULT '[]',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # Try to add optional columns if migrating from old schema
            try:
                conn.execute("ALTER TABLE task_state ADD COLUMN row_count INTEGER DEFAULT 0")
                conn.execute("ALTER TABLE task_state ADD COLUMN duration REAL DEFAULT 0.0")
                conn.execute("ALTER TABLE task_state ADD COLUMN qc_results TEXT DEFAULT '[]'")
            except sqlite3.OperationalError:
                pass
            conn.commit()

    def update_status(self, task_name: str, status: str, error_message: str = "", row_count: int = 0, duration: float = 0.0, qc_results: str = "[]"):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT INTO task_state (task_name, status, error_message, row_count, duration, qc_results, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(task_name) DO UPDATE SET 
                    status = excluded.status,
                    error_message = excluded.error_message,
                    row_count = excluded.row_count,
                    duration = excluded.duration,
                    qc_results = excluded.qc_results,
                    updated_at = CURRENT_TIMESTAMP
            ''', (task_name, status, error_message, row_count, duration, qc_results))
            conn.commit()

    def get_all_states(self) -> dict:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM task_state").fetchall()
            return {row["task_name"]: dict(row) for row in rows}

from fluxo.core.adapters import BaseAdapter, DuckDBAdapter, PostgresAdapter, BigQueryAdapter # type: ignore

class DataQualityEngine:
    def __init__(self, adapter: BaseAdapter):
        self.adapter = adapter

    def run_tests(self, task_name: str, tests) -> List[str]:
        """Runs quality tests on a table and returns a list of error messages. Empty list means success."""
        errors = []
        
        # not_null
        if tests.not_null:
            for col in tests.not_null:
                query = f"SELECT count(*) FROM {task_name} WHERE {col} IS NULL"
                try:
                    result = self.adapter.fetchone(query)[0]
                    if result > 0:
                        errors.append(f"not_null test failed on {col}: {result} nulls found")
                except Exception as e:
                    errors.append(f"Failed to run not_null test on {col}: {str(e)}")
                
        # unique
        if tests.unique:
            for col in tests.unique:
                query = f"SELECT count(*) FROM (SELECT {col} FROM {task_name} GROUP BY {col} HAVING count(*) > 1) AS duplicates"
                try:
                    result = self.adapter.fetchone(query)[0]
                    if result > 0:
                        errors.append(f"unique test failed on {col}: {result} duplicates found")
                except Exception as e:
                    errors.append(f"Failed to run unique test on {col}: {str(e)}")
                
        # accepted_values
        if hasattr(tests, 'accepted_values') and tests.accepted_values:
            for col, values in tests.accepted_values.items():
                formatted_values = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in values)
                query = f"SELECT count(*) FROM {task_name} WHERE {col} NOT IN ({formatted_values}) AND {col} IS NOT NULL"
                try:
                    result = self.adapter.fetchone(query)[0]
                    if result > 0:
                        errors.append(f"accepted_values test failed on {col}: {result} invalid values found")
                except Exception as e:
                    errors.append(f"Failed to run accepted_values test on {col}: {str(e)}")
                
        # row_count_min
        if tests.row_count_min is not None:
            query = f"SELECT count(*) FROM {task_name}"
            try:
                result = self.adapter.fetchone(query)[0]
                if result < tests.row_count_min:
                    errors.append(f"row_count_min test failed: expected >= {tests.row_count_min}, got {result}")
            except Exception as e:
                errors.append(f"Failed to run row_count_min test: {str(e)}")
                
        return errors

class FluxoRunner:
    def __init__(self, manifest: FluxoManifest, dry_run: bool = False):
        self.manifest = manifest
        self.dry_run = dry_run
        self.graph = FluxoGraph(manifest)
        self.state = StateManager()
        
        # Determine adapter based on configuration
        conn_type = self.manifest.adapter_config.get("connection_type", "duckdb")
        
        if conn_type == "postgres":
            self.adapter = PostgresAdapter(**{k: v for k, v in self.manifest.adapter_config.items() if k != "connection_type"})
        elif conn_type == "bigquery":
            self.adapter = BigQueryAdapter(**{k: v for k, v in self.manifest.adapter_config.items() if k != "connection_type"})
        else:
            db_path = self.manifest.adapter_config.get("db_path", "fluxo_target.duckdb")
            self.adapter = DuckDBAdapter(db_path=db_path, memory=self.dry_run)
            
        self.dq_engine = DataQualityEngine(self.adapter)

    def _table_exists(self, table_name: str) -> bool:
        """Helper to safely check if a table exists across adapters."""
        try:
            # We attempt a light count query. If it throws, table doesn't exist.
            res = self.adapter.fetchone(f"SELECT 1 FROM {table_name} LIMIT 1")
            return True
        except Exception:
            return False

    def send_webhook(self, payload: dict):
        if self.manifest.webhook_url:
            import urllib.request
            import json
            req = urllib.request.Request(
                self.manifest.webhook_url, 
                data=json.dumps(payload).encode('utf-8'),
                headers={'Content-Type': 'application/json'}
            )
            try:
                urllib.request.urlopen(req, timeout=5)
            except Exception as e:
                console.print(f"[yellow]Failed to send webhook: {e}[/yellow]")

    def print_emergency_report(self, task_name: str, error_msg: str):
        import time
        report = f"""[bold red]🚨 FLUXO PIPELINE HALTED 🚨[/bold red]
[bold]Task:[/bold] {task_name}
[bold]Time:[/bold] {time.strftime('%Y-%m-%d %H:%M:%S')}
        
[bold]Error Details:[/bold]
{error_msg}"""
        console.print(Panel(report, border_style="red", title="Emergency Report", expand=False))
        self.send_webhook({"text": f"🚨 Fluxo Pipeline Failed on Task: {task_name}\nError: {error_msg}"})

    def run_all(self):
        import time
        from pathlib import Path
        import os
        
        lock_file_path = Path(".fluxo.lock")
        if lock_file_path.exists():
            console.print("[bold red]Error: Another instance of Fluxo is already running![/bold red]")
            console.print("If this is a mistake, delete the '.fluxo.lock' file and try again.")
            raise RuntimeError("Concurrent execution blocked by .fluxo.lock")
            
        try:
            # Create lock file
            lock_file_path.touch()
            self._execute_all(time)
        finally:
            if lock_file_path.exists():
                lock_file_path.unlink()
                
    def _execute_all(self, time_module):
        time = time_module
        import concurrent.futures
        
        execution_order = self.graph.get_execution_order()
        
        # Initialize all to Pending
        for task in execution_order:
            self.state.update_status(task.name, "Pending")

        completed_tasks = set()
        failed_tasks = set()
        futures = {}
        
        def run_task(task):
            return self._run_single_task(task, time)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            while len(completed_tasks) + len(failed_tasks) < len(execution_order):
                # Find tasks whose dependencies are met
                for task in execution_order:
                    if task.name in completed_tasks or task.name in failed_tasks or task.name in futures:
                        continue
                    
                    can_run = all(dep in completed_tasks for dep in task.depends_on)
                    if can_run:
                        futures[task.name] = executor.submit(run_task, task) # type: ignore
                
                # Wait for at least one future to complete
                done, _ = concurrent.futures.wait(futures.values(), return_when=concurrent.futures.FIRST_COMPLETED)
                
                for future in done:
                    # Find which task this future belonged to
                    task_name = next(name for name, f in futures.items() if f == future)
                    try:
                        success = future.result()
                        if success:
                            completed_tasks.add(task_name)
                        else:
                            failed_tasks.add(task_name)
                    except Exception as e:
                        console.print(f"[bold red]Unexpected task failure for {task_name}: {e}[/bold red]")
                        failed_tasks.add(task_name)
                    
                    futures.pop(task_name, None)
                
                if failed_tasks:
                   console.print("[bold red]Pipeline halted due to task failure.[/bold red]")
                   break

    def _run_single_task(self, task, time) -> bool:
        console.print(f"Running task: [bold cyan]{task.name}[/bold cyan]")
        self.state.update_status(task.name, "Running")
        start_time = time.time()
        
        root = Path(self.manifest.project_root) if getattr(self.manifest, 'project_root', None) else Path.cwd()
        sql_path = root / task.source_sql
        if not sql_path.exists():
            error_msg = f"SQL file not found: {sql_path}"
            self.print_emergency_report(task.name, error_msg)
            self.state.update_status(task.name, "Failed", error_msg)
            return False

        try:
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_query = f.read()

            if self.dry_run:
                try:
                    import sqlglot # type: ignore
                    # Syntax block parsing
                    stmts = sqlglot.parse(sql_query)
                    dry_stmts = []
                    for stmt in stmts:
                        # Apply LIMIT 0 to Select expression to validate schema quickly
                        if isinstance(stmt, sqlglot.exp.Create) and stmt.expression and isinstance(stmt.expression, sqlglot.exp.Select):
                            stmt.expression.limit(0, copy=False)
                        elif isinstance(stmt, sqlglot.exp.Insert) and stmt.expression and isinstance(stmt.expression, sqlglot.exp.Select):
                            stmt.expression.limit(0, copy=False)
                        elif isinstance(stmt, sqlglot.exp.Select):
                            stmt.limit(0, copy=False)

                        dry_stmts.append(stmt.sql(dialect="duckdb"))
                    
                    # Validate schema references
                    for d_sql in dry_stmts:
                        self.adapter.execute(d_sql)
                        
                    self.state.update_status(task.name, "Success (Dry Run)")
                    console.print(f"Task [bold cyan]{task.name}[/bold cyan] [bold yellow]Valid (Dry Run)[/bold yellow]")
                    return True
                except Exception as e:
                    error_msg = f"Dry Run Validation Failed: {str(e)}"
                    self.print_emergency_report(task.name, error_msg)
                    self.state.update_status(task.name, "Failed", error_msg)
                    return False
            
            # Build Materialization Query
            final_sql = sql_query
            if not self.dry_run:
                if task.materialized == "incremental" and self._table_exists(task.name):
                    if task.timestamp_col:
                        try:
                            max_ts_row = self.adapter.fetchone(f"SELECT MAX({task.timestamp_col}) FROM {task.name}")
                            max_ts = max_ts_row[0] if max_ts_row else None
                            
                            # Subquery the user's logic, filtering by timestamp
                            final_sql = f"""
                            INSERT INTO {task.name}
                            SELECT * FROM (
                                {sql_query}
                            ) AS _inc_source
                            """
                            if max_ts:
                                # Very basic string encapsulation for now
                                ts_lit = f"'{max_ts}'" if isinstance(max_ts, str) else str(max_ts)
                                final_sql += f" WHERE _inc_source.{task.timestamp_col} > {ts_lit}"
                        except Exception as e:
                            raise ValueError(f"Failed to build incremental logic: {e}")
                elif task.materialized == "view":
                    final_sql = f"CREATE OR REPLACE VIEW {task.name} AS \n{sql_query}"
                elif task.materialized == "table":
                    final_sql = f"CREATE OR REPLACE TABLE {task.name} AS \n{sql_query}"
                else:
                    # Ensure we default to creating the table if nothing else specified and it's not a direct insert
                    if not sql_query.strip().upper().startswith(("CREATE", "INSERT", "UPDATE", "DELETE")):
                        final_sql = f"CREATE OR REPLACE TABLE {task.name} AS \n{sql_query}"

            # Execute the SQL using the adapter with retry logic
            max_retries = 3
            for attempt in range(1, max_retries + 1):
                try:
                    self.adapter.execute(final_sql)
                    break
                except Exception as adapter_ex:
                    if attempt == max_retries:
                        raise RuntimeError(f"Database execution failed after {max_retries} attempts: {adapter_ex}")
                    console.print(f"[yellow]Query failed (Attempt {attempt}/{max_retries}). Retrying in 2 seconds...[/yellow]")
                    time.sleep(2)
            
            # Run Data Quality Tests
            test_errors = self.dq_engine.run_tests(task.name, task.tests)
            duration = time.time() - start_time
            qc_json = json.dumps(test_errors)
            
            try:
                row_count = self.adapter.get_row_count(task.name)
            except Exception:
                row_count = 0

            if test_errors:
                error_msg = "Data Quality Tests Failed:\n" + "\n".join(f"- {err}" for err in test_errors)
                self.print_emergency_report(task.name, error_msg)
                self.state.update_status(task.name, "Failed", error_msg, row_count, duration, qc_json)
                return False

            self.state.update_status(task.name, "Success", "", row_count, duration, qc_json)
            console.print(f"Task [bold cyan]{task.name}[/bold cyan] [bold green]Success[/bold green] ({duration:.2f}s)")
            return True
        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            self.print_emergency_report(task.name, error_msg)
            self.state.update_status(task.name, "Failed", error_msg, 0, duration, "[]")
            return False
