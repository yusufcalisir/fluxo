import networkx as nx # type: ignore
from typing import List, Dict
from fluxo.core.parser import FluxoManifest, FluxoTask # type: ignore

class FluxoGraph:
    def __init__(self, manifest: FluxoManifest):
        self.manifest = manifest
        self.graph = nx.DiGraph()
        self.tasks_map: Dict[str, FluxoTask] = {task.name: task for task in manifest.tasks}
        self._build_graph()

    def _build_graph(self):
        """Constructs the Directed Acyclic Graph based on dependencies."""
        for task in self.manifest.tasks:
            self.graph.add_node(task.name)
            for dependency in task.depends_on:
                if dependency not in self.tasks_map:
                    raise ValueError(f"Task '{task.name}' depends on unknown task '{dependency}'")
                # Directed edge from dependency to task
                self.graph.add_edge(dependency, task.name)

        try:
            if not nx.is_directed_acyclic_graph(self.graph):
                cycles = list(nx.simple_cycles(self.graph))
                raise ValueError(f"Circular dependency detected in tasks: {cycles}")
        except nx.NetworkXUnfeasible as e:
            raise ValueError(f"DAG is unfeasible due to circular dependencies: {e}")

    def get_execution_order(self) -> List[FluxoTask]:
        """Returns the topologically sorted tasks."""
        sorted_names = list(nx.topological_sort(self.graph))
        return [self.tasks_map[name] for name in sorted_names]
