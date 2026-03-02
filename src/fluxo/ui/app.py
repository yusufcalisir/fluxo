import streamlit as st # type: ignore
import pandas as pd # type: ignore
import duckdb # type: ignore
from streamlit_agraph import agraph, Node, Edge, Config # type: ignore
from pathlib import Path
import sys
import os
import sqlite3
import time
import subprocess

# Ensure the local package is importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from fluxo.core.parser import parse_manifest # type: ignore
from fluxo.core.graph import FluxoGraph # type: ignore
from fluxo.core.runner import StateManager, FluxoRunner # type: ignore

# --- THEME CONFIGURATION ---
st.set_page_config(
    page_title="Fluxo | Data Engine",
    layout="wide",
    page_icon="🌊",
    initial_sidebar_state="expanded"
)

# Palette - Premium Dark Mode
ELECTRIC_EMERALD = "#10B981"
MIDNIGHT_CHARCOAL = "#0B0F19"
FROST_WHITE = "#F9FAFB"
MODERN_BLUE = "#3B82F6"
VIVID_RED = "#EF4444"
SUBTLE_GREY = "#9CA3AF"
CARD_BG = "#111827"
BORDER_COLOR = "#1F2937"

STATUS_COLORS = {
    "Pending": SUBTLE_GREY,
    "Running": MODERN_BLUE,
    "Success": ELECTRIC_EMERALD,
    "Failed": VIVID_RED
}

# --- PREMIUM UI CSS ---
st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* Global Theme Overrides - No Unnecessary Scrolls */
    * {{
        font-family: 'Inter', sans-serif !important;
        scrollbar-width: none !important;
        -ms-overflow-style: none !important;
    }}
    
    ::-webkit-scrollbar {{
        display: none !important;
    }}

    html, body, .stApp, [data-testid="stAppViewContainer"], [data-testid="stHeader"] {{
        background-color: {MIDNIGHT_CHARCOAL} !important;
        color: {FROST_WHITE} !important;
        overflow: hidden !important; 
    }}
    
    [data-testid="stAppViewContainer"] > section:first-child {{
        overflow-y: auto !important; 
        overflow-x: hidden !important;
    }}

    /* Layout tweaks to remove extra white space causing scrolls */
    .block-container {{
        padding-top: 2rem !important;
        padding-bottom: 2rem !important;
        padding-left: 3rem !important;
        padding-right: 3rem !important;
        max-width: 100% !important;
    }}

    /* Typography Excellence */
    h1, h2, h3, h4, .stSubheader {{
        color: {FROST_WHITE} !important;
        font-weight: 600 !important;
        letter-spacing: -0.025em !important;
    }}

    h1 {{
        font-size: 2.25rem !important;
        margin-bottom: 1.5rem !important;
    }}

    /* Distinct & Clear Components (No blur/glass) */
    .clean-card {{
        background-color: {CARD_BG};
        border: 1px solid {BORDER_COLOR};
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        margin-bottom: 1.5rem;
    }}

    /* Metrics Pulse */
    [data-testid="stMetricValue"] {{
        color: {FROST_WHITE} !important;
        font-size: 2rem !important;
        font-weight: 700 !important;
    }}
    [data-testid="stMetricLabel"] {{
        color: {SUBTLE_GREY} !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-size: 0.75rem !important;
    }}

    /* NUCLEAR: Eliminate White Boxes */
    iframe, .stAgraph, div[data-testid="stHtml"], [data-testid="stVerticalBlock"] > div {{
        background-color: transparent !important;
    }}
    
    /* Agraph Container */
    [data-testid="stVerticalBlock"] > div:has(iframe[title="streamlit_agraph.agraph"]) {{
        background-color: {CARD_BG} !important;
        border: 1px solid {BORDER_COLOR} !important;
        border-radius: 12px !important;
        padding: 0 !important;
        overflow: hidden !important;
    }}

    /* Sidebar Refinement */
    [data-testid="stSidebar"] {{
        background-color: {CARD_BG} !important;
        border-right: 1px solid {BORDER_COLOR} !important;
    }}
    
    /* Tab Elegance */
    .stTabs [data-baseweb="tab-list"] {{
        background-color: transparent !important;
        border-bottom: 2px solid {BORDER_COLOR} !important;
        gap: 24px;
    }}
    
    .stTabs [data-baseweb="tab"] {{
        color: {SUBTLE_GREY} !important;
        font-weight: 500 !important;
        padding: 12px 4px !important;
        background-color: transparent !important;
        border: none !important;
    }}
    
    .stTabs [aria-selected="true"] {{
        color: {MODERN_BLUE} !important;
        border-bottom: 2px solid {MODERN_BLUE} !important;
    }}

    /* Code Editor Visuals */
    .stCodeBlock {{
        border-radius: 8px !important;
        border: 1px solid {BORDER_COLOR} !important;
        background-color: rgba(0,0,0,0.2) !important;
    }}

    /* Buttons */
    .stButton > button[kind="primary"] {{
        background-color: {MODERN_BLUE} !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 500 !important;
        padding: 0.5rem 1rem !important;
    }}
    .stButton > button[kind="secondary"] {{
        background-color: transparent !important;
        color: {FROST_WHITE} !important;
        border: 1px solid {BORDER_COLOR} !important;
        border-radius: 8px !important;
        font-weight: 500 !important;
        padding: 0.5rem 1rem !important;
    }}

    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    header {{visibility: hidden;}}
</style>
""", unsafe_allow_html=True)

# Context Loading
manifest_path = "fluxo.yaml"
try:
    manifest = parse_manifest(manifest_path)
    graph = FluxoGraph(manifest)
    state_manager = StateManager()
    all_states = state_manager.get_all_states()
    tasks = graph.get_execution_order()
    is_online = True
except Exception as e:
    is_online = False
    error_detail = str(e)
    tasks = []
    all_states = {}

def get_state(name: str) -> dict:
    s = all_states.get(name)
    return s if isinstance(s, dict) else {}

# --- SIDEBAR & BRANDING ---
with st.sidebar:
    # Text-based CSS logo to fix any image background issues completely
    st.markdown("""
        <div style="display: flex; align-items: center; gap: 12px; padding: 10px 0 20px 0;">
            <div style="background: linear-gradient(135deg, #3B82F6, #10B981); border-radius: 12px; width: 44px; height: 44px; display: flex; align-items: center; justify-content: center; font-size: 24px; font-weight: bold; color: white; box-shadow: 0 4px 10px rgba(59, 130, 246, 0.3);">
                F
            </div>
            <div style="font-size: 26px; font-weight: 700; color: #F9FAFB; letter-spacing: -0.5px;">
                Fluxo Engine
            </div>
        </div>
    """, unsafe_allow_html=True)
    
    # PULSE Check
    with st.container():
        st.markdown('<div class="pulse-header">ENGINE PULSE</div>', unsafe_allow_html=True)
        
        # CORRECTED METRICS LOGIC
        total_tasks = len(tasks)
        
        # Deduplicated Success Count: only count the latest successful state if it exists for a current task
        success_count = sum(1 for task_name, state in all_states.items() 
                          if state["status"] == "Success" and any(t.name == task_name for t in tasks))
        
        success_rate = (success_count / total_tasks * 100) if total_tasks > 0 else 0
        last_run = max([t["updated_at"] for t in all_states.values()]) if all_states else "Never"
        
        c1, c2 = st.columns(2)
        with c1:
            st.metric("MODELS", total_tasks)
        with c2:
            # Bug fix: cap at 100% and ensure it reflects current manifest
            st.metric("SUCCESS", f"{min(success_rate, 100.0):.0f}%")
            
        st.markdown(f"**Uptime Check:** 🟢 Online")
        st.markdown(f"**Last Sync:** `{last_run.split('.')[0] if '.' in last_run else last_run}`")

    st.markdown("---")
    
    # ACTIONS
    st.subheader("Interactive Controls")
    search_query = st.text_input("🔍 Search Lineage", placeholder="Filter nodes...").strip().lower()
    
    if st.button("🚀 Run Pipeline", type="primary", use_container_width=True):
        with st.spinner("Executing Fluxo Pipeline..."):
            try:
                # Trigger runner
                runner = FluxoRunner(manifest)
                runner.run_all()
                st.success("Pipeline Run Complete!")
                time.sleep(1)
                st.rerun()
            except Exception as ex:
                st.error(f"Run Failed: {ex}")

    if st.button("🔄 Refresh Data", type="secondary", use_container_width=True):
        st.rerun()

# --- MAIN DASHBOARD ---
st.markdown('<h1 style="margin-top: -50px;">Operations Hub</h1>', unsafe_allow_html=True)

if not is_online:
    st.error("🚨 Connection Lost / System Offline")
    st.warning(f"Fluxo Engine failed to initialize: {error_detail}")
    st.info("Check project configuration and database availability.")
    st.stop()

tab_graph, tab_logs, tab_timeline, tab_profiling, tab_history = st.tabs([
    "📊 Graph Lineage", 
    "📜 Execution Logs", 
    "⏱ Timeline", 
    "📈 Data Profiling", 
    "📚 Run History"
])

with tab_graph:
    col_graph, col_detail = st.columns([3, 1])
    
    with col_graph:
        nodes = []
        edges = []
        
        for task in tasks:
            if search_query and search_query not in task.name.lower():
                continue
                
            status = get_state(task.name).get("status", "Pending")
            color = STATUS_COLORS.get(status, SUBTLE_GREY)
            
            is_active = status == "Running"
            
            nodes.append(Node(
                id=task.name,
                label=task.name,
                size=35 if is_active else 30,
                color=color,
                highlightColor=ELECTRIC_EMERALD,
                font={"color": "white", "size": 18, "face": "Inter", "background": "rgba(0,0,0,0.5)"},
            ))
            
            for dep in task.depends_on:
                if not search_query or (search_query in task.name.lower() or search_query in dep.lower()):
                    edges.append(Edge(source=dep, target=task.name, color="rgba(255,255,255,0.3)", width=2))
                
        config = Config(
            width="100%",
            height=600, # Reduced to avoid scrolling
            directed=True,
            physics=True,
            hierarchical=False,
            nodeHighlightBehavior=True,
            collapsible=False,
            options={
                "physics": {
                    "forceAtlas2Based": {
                        "gravitationalConstant": -100,
                        "centralGravity": 0.01,
                        "springLength": 150,
                        "springConstant": 0.08,
                    },
                    "maxVelocity": 50,
                    "solver": "forceAtlas2Based",
                    "timestep": 0.35,
                    "stabilization": {"iterations": 150}
                },
                "interaction": {
                    "zoomView": True,
                    "dragView": True,
                }
            }
        )
        
        clicked_node = agraph(nodes=nodes, edges=edges, config=config)

    with col_detail:
        st.markdown('<div class="clean-card">', unsafe_allow_html=True)
        st.subheader("Inspector")
        if clicked_node:
            task_obj = next((t for t in tasks if t.name == clicked_node), None)
            if task_obj:
                st.markdown(f"### `{task_obj.name}`")
                status = get_state(task_obj.name).get("status", "Pending")
                st.write(f"**Status:** {status}")
                
                m1, m2 = st.columns(2)
                m1.markdown(f"**Rows**\n{get_state(task_obj.name).get('row_count', 0)}")
                m2.markdown(f"**Runtime**\n{get_state(task_obj.name).get('duration', 0.0):.2f}s")
                
                qc = get_state(task_obj.name).get("qc_results", "[]")
                if qc != "[]":
                    import json
                    try:
                        qc_data = json.loads(qc)
                        if qc_data:
                            st.markdown("---")
                            st.markdown("⚠️ **Quality Violations**")
                            for err in qc_data:
                                st.caption(f"❌ {err}")
                    except Exception:
                        pass
                
                st.markdown("---")
                st.caption("SQL Source")
                st.code(task_obj.sql_content or "-- No SQL loaded", language="sql")
        else:
            st.info("Select a node on the map to inspect properties.")
        st.markdown('</div>', unsafe_allow_html=True)

with tab_logs:
    st.subheader("Event Stream")
    for task in tasks:
        state = get_state(task.name)
        status = state.get("status", "Pending")
        updated = state.get("updated_at", "-")
        err = state.get("error_message", "")
        
        with st.expander(f"{task.name} • {status} • {updated}", expanded=(status == "Failed")):
            if err:
                st.error(f"Error Log:\n{err}")
            elif status == "Success":
                st.success(f"Final Validation: Success at {updated}")
            else:
                st.info(f"State: {status}")

with tab_timeline:
    st.subheader("Performance Profile")
    durations = [{"Task": t.name, "Duration (s)": get_state(t.name).get("duration", 0.0)} for t in tasks]
    df_durations = pd.DataFrame(durations)
    if not df_durations.empty and df_durations["Duration (s)"].sum() > 0:
        st.bar_chart(df_durations, x="Task", y="Duration (s)", color=ELECTRIC_EMERALD)
    else:
        st.info("Performance metrics will populate after next execution.")

with tab_profiling:
    st.subheader("Data Explorer")
    try:
        from fluxo.core.adapters import DuckDBAdapter, PostgresAdapter, BigQueryAdapter # type: ignore
        conn_type = manifest.adapter_config.get("connection_type", "duckdb")
        
        adapter = None
        if conn_type == "postgres":
            adapter = PostgresAdapter(**{k: v for k, v in manifest.adapter_config.items() if k != "connection_type"})
        elif conn_type == "bigquery":
            adapter = BigQueryAdapter(**{k: v for k, v in manifest.adapter_config.items() if k != "connection_type"})
        else:
            db_path = manifest.adapter_config.get("db_path", "fluxo_target.duckdb")
            if Path(db_path).exists():
                adapter = DuckDBAdapter(db_path=db_path)
        
        if adapter:
            success_tasks = [t.name for t in tasks if all_states.get(t.name, {}).get("status") == "Success"]
            if success_tasks:
                target_table = st.selectbox("Load Table Preview", success_tasks)
                if target_table:
                    res = adapter.fetchall(f"SELECT * FROM {target_table} LIMIT 25")
                    if res:
                        st.dataframe(pd.DataFrame(res), use_container_width=True)
                    else:
                        st.info("Table is currently empty.")
            else:
                st.info("No successful models available for profiling.")
            adapter.close()
    except Exception as e:
        st.error(f"Adapter connection interrupted: {e}")

with tab_history:
    st.subheader("Run Registry")
    try:
        with sqlite3.connect(state_manager.db_path) as conn:
            df_history = pd.read_sql_query("SELECT task_name, status, row_count, duration, updated_at FROM task_state ORDER BY updated_at DESC", conn)
            st.dataframe(df_history, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Traceability log unavailable: {e}")
