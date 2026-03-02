import typer # type: ignore
from rich.console import Console # type: ignore
from fluxo.core.parser import parse_manifest # type: ignore

app = typer.Typer(help="Fluxo: Modern Data Orchestration & ELT")
console = Console()

@app.command()
def run(
    manifest: str = typer.Option("fluxo.yaml", help="Path to the fluxo.yaml manifest"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate SQL without actually running it")
):
    """
    Execute the data pipeline defined in the manifest.
    """
    console.print(f"[bold green]Starting Fluxo Run[/bold green] using {manifest}...")
    try:
        from fluxo.core.runner import FluxoRunner # type: ignore
        parsed_manifest = parse_manifest(manifest)
        console.print(f"Loaded {len(parsed_manifest.tasks)} tasks from manifest.")
        runner = FluxoRunner(parsed_manifest, dry_run=dry_run)
        runner.run_all()
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1)

@app.command()
def init(
    project_name: str = typer.Argument(..., help="Name of the new Fluxo project")
):
    """
    Initialize a new Fluxo project with boilerplate scaffolding.
    """
    import os
    from pathlib import Path
    
    console.print(f"[bold green]Initializing new Fluxo project:[/bold green] {project_name}")
    
    base_dir = Path(project_name)
    if base_dir.exists():
        console.print(f"[bold red]Error:[/bold red] Directory '{project_name}' already exists.")
        raise typer.Exit(code=1)
        
    # Create Folders
    os.makedirs(base_dir / "models")
    os.makedirs(base_dir / "tests")
    
    # Create fluxo.yaml
    fluxo_yaml_content = f"""# Fluxo Pipeline Manifest
profile_target: dev

tasks:
  - name: example_model
    source_sql: models/example.sql
    materialized: table
    tests:
      not_null:
        - id
"""
    with open(base_dir / "fluxo.yaml", "w") as f:
        f.write(fluxo_yaml_content)
        
    # Create .env example
    env_content = """# Environment Variables for Fluxo
DB_PASSWORD=my_super_secure_password
"""
    with open(base_dir / ".env", "w") as f:
        f.write(env_content)
        
    # Create profiles.yaml
    profiles_yaml_content = """# Fluxo Database Profiles
dev:
  connection_type: duckdb
  db_path: fluxo_target.duckdb
  
# example_postgres:
#   connection_type: postgres
#   host: localhost
#   user: myuser
#   password: ${DB_PASSWORD}
"""
    with open(base_dir / "profiles.yaml", "w") as f:
        f.write(profiles_yaml_content)

    # Create an example model
    example_sql = """-- Example Fluxo Model
SELECT 
    1 AS id,
    'Alice' AS user_name,
    CURRENT_TIMESTAMP AS created_at
"""
    with open(base_dir / "models" / "example.sql", "w") as f:
        f.write(example_sql)
        
    console.print(f"[bold cyan]Success![/bold cyan] Project scaffolded at ./{project_name}")
    console.print(f"Run [bold]`cd {project_name}`[/bold] and [bold]`fluxo run`[/bold] to get started.")

@app.command()
def ui():
    """
    Launch the Fluxo Streamlit UI dashboard.
    """
    console.print("[bold green]Starting Fluxo UI...[/bold green]")
    import subprocess
    import sys
    from pathlib import Path
    
    # Locate the app.py file relative to this script
    ui_script = Path(__file__).parent / "ui" / "app.py"
    if not ui_script.exists():
        console.print(f"[bold red]Error:[/bold red] UI script not found at {ui_script}")
        raise typer.Exit(code=1)
        
    subprocess.run([sys.executable, "-m", "streamlit", "run", str(ui_script)])

if __name__ == "__main__":
    app()
