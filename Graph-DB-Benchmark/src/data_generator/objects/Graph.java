package data_generator.objects;
import java.util.ArrayList;

public class Graph {
	public Adyacencia nodes[];
	
	public Graph(ArrayList<Edge> edges, int node_db_size){
		this.nodes = new Adyacencia[node_db_size+1];
		
		for (Edge e : edges){
			if(nodes[e.ID_node_1] == null)
				nodes[e.ID_node_1] = new Adyacencia(e.ID_node_1);
			nodes[e.ID_node_1].add(e.ID_node_2);
		}
	}
	public Graph(int node_db_size){
		this.nodes = new Adyacencia[node_db_size+1];
	}
	public void add_edges(ArrayList<Edge> edges){
		for (Edge e : edges){
			if(nodes[e.ID_node_1] == null)
				nodes[e.ID_node_1] = new Adyacencia(e.ID_node_1);
			nodes[e.ID_node_1].add(e.ID_node_2);
		}
	}
	public void balance_pruning(){
	}
	
	public String toString(){
		String s = "";
		for(Adyacencia a : nodes){
			if(a != null)
				s += a + "\n";
		}
		return s;
	}
	public void pruning(int id_node_a, int id_node_b) {
		nodes[id_node_a].adyacencias.remove(nodes[id_node_a].adyacencias.indexOf(id_node_b));
		nodes[id_node_b].adyacencias.remove(nodes[id_node_b].adyacencias.indexOf(id_node_a));		
	}
	public ArrayList<Edge> getUndirectedEdges() {
		ArrayList<Edge> edges = new ArrayList<Edge>();
		
		for(Adyacencia a : nodes){
			if(a != null){
				for(int i = 0; i < a.adyacencias.size(); i++){
					if(a.adyacencias.get(i) != null && a.key > a.adyacencias.get(i)){
						edges.add(new Edge(edges.size(), a.key, 0, a.adyacencias.get(i)));
					}
				}
			}
		}
		
		return edges;
	}
	public void add_edge(Edge e) {
		if(nodes[e.ID_node_1] == null)
			nodes[e.ID_node_1] = new Adyacencia(e.ID_node_1);
		nodes[e.ID_node_1].add(e.ID_node_2);
		
	}
}