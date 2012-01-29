package data_generator.converter;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Queue;

import data_generator.database.POSTGRESQL_DATABASE_HI5;
import data_generator.database.POSTGRESQL_DATABASE_HI5G;
import data_generator.exporter.DOT_EXPORT;

import data_generator.objects.Edge;
import data_generator.objects.Graph;
import data_generator.objects.Node;

public class GRAPH_GENERATOR {
	public static int N = 0;
	public static float PROPORTION = 0;
	public static float AVERANGE_DEGREE = 0;
	public static int ID_MAX_OUT_DEGREE = 0;
	public static int NODE_DB_SIZE = 0;
	public static int COUNT = 0;
	public static int contador = 0;
	
	public static Graph g;
	
	public static Hashtable<Integer, Integer> nodes = new Hashtable<Integer, Integer>();
	//public static ArrayList<Integer> queue = new ArrayList<Integer>();
	@SuppressWarnings("unchecked")
	public static Queue<Integer> queue = new LinkedList();
	/**
	 * @param args[1]: n
	 */
	public static void main(String[] args) {
		POSTGRESQL_DATABASE_HI5G.connect();
		//with_algorithm(args);
		//pruning_algorithm(args);
		//strong_relationship_algorithm(args);
		//optimization_algorithm(args);
//		DOT_EXPORT.open();
//		DOT_EXPORT.save_undirected_edges(POSTGRESQL_DATABASE_HI5G.getUnirectedEdges());
//		DOT_EXPORT.close();
		
		DOT_EXPORT.open("Original_Graph.dot");
		DOT_EXPORT.save_undirected_edges(POSTGRESQL_DATABASE_HI5G.getUndirectedEdges());
		DOT_EXPORT.close();
		
		DOT_EXPORT.open("With_Graph.dot");
		g = new Graph(31);
		with_algorithm(args);
		DOT_EXPORT.save_undirected_edges(g.getUndirectedEdges());
		DOT_EXPORT.close();
		
		DOT_EXPORT.open("Pruning_Graph.dot");
		g = new Graph(31);
		pruning_algorithm(args);
		DOT_EXPORT.save_undirected_edges(g.getUndirectedEdges());
		DOT_EXPORT.close();
		
		DOT_EXPORT.open("Strong_Graph.dot");
		g = new Graph(31);
		strong_relationship_algorithm(args);
		DOT_EXPORT.save_undirected_edges(g.getUndirectedEdges());
		DOT_EXPORT.close();
		
		DOT_EXPORT.open("Optimization_Graph.dot");
		g = new Graph(31);
		optimization_algorithm(args);
		DOT_EXPORT.save_undirected_edges(g.getUndirectedEdges());
		DOT_EXPORT.close();
		
		
		
		POSTGRESQL_DATABASE_HI5G.disconnect();
	}
	public static void optimization_algorithm(String args[]){
		//String n = args[0];
		String n = "15";
		N = Integer.parseInt(n);
		
		AVERANGE_DEGREE = POSTGRESQL_DATABASE_HI5G.get_AVERANGE_DEGREE();
		ID_MAX_OUT_DEGREE = POSTGRESQL_DATABASE_HI5G.get_ID_max_out_degree();
		NODE_DB_SIZE = POSTGRESQL_DATABASE_HI5G.get_NODE_DB_size();
		PROPORTION = (float)N / (float)NODE_DB_SIZE;

		g = new Graph(POSTGRESQL_DATABASE_HI5G.get_PRUNING(N), NODE_DB_SIZE);
		balance_pruning();
		
		System.out.println("FINAL GRAPH:");
		System.out.println(g);
		
	}
	public static int[] getDegrees(){
		int[] degrees = new int[NODE_DB_SIZE+1];
		for(int i = 0; i < g.nodes.length; i++){
			if(g.nodes[i] != null)
				degrees[i] = getDegreeOf(g.nodes[i].key);
		}
		return degrees;
	}
	public static void balance_pruning() {
		int[] degree_differences = calculateDifferences(getDegrees());
		for(int i = 0; i < degree_differences.length; i++){
			if(degree_differences[i] < 0){
				//System.out.println(i + "::" + degree_differences[i]);
				for(int j = 0; j < g.nodes[i].adyacencias.size() && degree_differences[i] < 0; j++){
					if(degree_differences[g.nodes[i].adyacencias.get(j)] < 0){
						int ady = g.nodes[i].adyacencias.get(j);
						g.pruning(i, ady);
						degree_differences[i]++;
						degree_differences[ady]++;
						//System.out.println("*Corto: " + i + " " + ady);
					}
				}
			}
		}
//		for(int i = 0; i < degree_differences.length; i++){
//			if(degree_differences[i]<-1)
//				System.out.println(i + ":" + degree_differences[i]);
//		}
	}
	public static int[] calculateDifferences(int[] degrees){
		int[] differences = new int[degrees.length];

		//System.out.println(degrees.length + " " + g.adyacencias.length);
		for(int i = 0; i < degrees.length; i++){
			if(degrees[i] > 0){
				int new_degree = (int)(degrees[i]*PROPORTION);
				//System.out.println("NODE:" + i + " PROPORTION:" + PROPORTION + " R_DEGREE:" + degrees[i] + " N_DEGREE:" + g.nodes[i].adyacencias.size() + " I_DEGREE:" + new_degree);
				differences[i] = new_degree - g.nodes[i].adyacencias.size();			
			}
		}
		return differences;
	}
	public static void strong_relationship_algorithm(String args[]){
		//String n = args[0];
		String n = "15";
		N = Integer.parseInt(n);
		
		
		
		AVERANGE_DEGREE = POSTGRESQL_DATABASE_HI5G.get_AVERANGE_DEGREE();
		ID_MAX_OUT_DEGREE = POSTGRESQL_DATABASE_HI5G.get_ID_max_out_degree();
		NODE_DB_SIZE = POSTGRESQL_DATABASE_HI5G.get_NODE_DB_size();
		
		PROPORTION = (float)N / (float)NODE_DB_SIZE;
		
		System.out.println("Max ID degree: " +ID_MAX_OUT_DEGREE);
		System.out.println("Size: " + NODE_DB_SIZE);
		System.out.println("Proporcion: " + PROPORTION);
		System.out.println("Averange: " + AVERANGE_DEGREE);
		strong_relationship_save_friends(ID_MAX_OUT_DEGREE);
		
		
//		ArrayList<Integer> a = new ArrayList<Integer>();
//		for(int i = 0; i < 40000000; i++){
//			a.add(i);
//		}
//		System.out.println(a.size());
	}
	public static ArrayList<Node> getFriendsOf(int id){
		 return POSTGRESQL_DATABASE_HI5G.get_FRIENDS(id);
	}
	public static int getDegreeOf(int id){
		 return POSTGRESQL_DATABASE_HI5G.get_degree_of(id);
	}
	public static void strong_relationship_save_friends(int id){
		nodes.put(id, id);
		
		ArrayList<Node> id_friends = getFriendsOf(id);
		
		int proportion_friends =(int)(id_friends.size()*PROPORTION);
		
//		System.out.println(id_friends.size());
		
		float influential_percentage = calculate_influential(id_friends);
		
		int influential_friends = (int)((proportion_friends*influential_percentage)/100.0);
		int isolated_friends = proportion_friends - influential_friends;
		
		
		
//		System.out.println("Influyentes: " + influential_percentage);
//		System.out.println("Amigos Influyentes: " + influential_friends);
//		System.out.println("Aislado: " + (100 - influential_percentage));
//		System.out.println("Amigos Aislados: " + isolated_friends);
		
		int count_influential = 0, count_isolated = 0;
		for(Node id_friend : id_friends){
			if(contador < N){
				if(id_friend.out_degree >= AVERANGE_DEGREE && count_influential < influential_friends && !nodes.contains(id_friend.ID) && !queue.contains(id_friend.ID)){
					save_edge(id, id_friend.ID);
					count_influential++;
				}
				else if(id_friend.out_degree < AVERANGE_DEGREE && count_isolated < isolated_friends && !nodes.contains(id_friend.ID) && !queue.contains(id_friend.ID)){
					save_edge(id, id_friend.ID);
					count_isolated++;
				}
			}
		}
		
		if(queue.size() > 0 && contador < N){
			int nid = queue.remove();
			strong_relationship_save_friends(nid);
		}
		else if(contador == N){
			System.out.println("Se ha terminado la generación del grafo");
		}
		else if(contador < N){
			System.out.println("PROBLEMAS!! Aparentemente el grafo está cortado, y no se ha podido generar uno proporcional");
		}
	}
	private static void save_edge(int id_1, int id_2){
		if(!nodes.contains(id_2) && !queue.contains(id_2)){
			queue.add(id_2);
			contador++;
		}
		g.add_edge(new Edge(0, id_1, 0, id_2));
		g.add_edge(new Edge(0, id_2, 0, id_1));
		System.out.println(id_1 + " ------ " + id_2);

			

			
//		System.out.print("Nodos: ");
//		Enumeration<Integer> e = nodes.keys();
//		while(e.hasMoreElements())
//			  System.out.print(e.nextElement()+" ");
//		System.out.println();
//		
//		System.out.print("Queue: ");
//		for(int q : queue){
//			System.out.print(q+" ");
//		}
//		System.out.println();
		
		
		
	}
	private static float calculate_influential(ArrayList<Node> friends){
		int influential = 0;
		for(Node friend : friends)
			if(friend.out_degree > AVERANGE_DEGREE)
				influential++;
		return ((float)influential / (float)friends.size()) * 100;
	}
	public static void pruning_algorithm(String args[]){
		String n = "15";
		N = Integer.parseInt(n);
		
		ArrayList<Edge> edges = POSTGRESQL_DATABASE_HI5G.get_PRUNING(N);
		g.add_edges(edges);
		for (Edge e : edges){
			System.out.println(e.ID_node_1 + " - " + e.ID_node_2);
		}
		
	
	}
	public static void proportional_algorithm(String args[]){
		String n = args[0];
		N = Integer.parseInt(n);
		
		POSTGRESQL_DATABASE_HI5.connect();
		
		ID_MAX_OUT_DEGREE = POSTGRESQL_DATABASE_HI5.get_ID_max_out_degree();
		NODE_DB_SIZE = POSTGRESQL_DATABASE_HI5.get_NODE_DB_size();
		
		PROPORTION = (float)N / (float)NODE_DB_SIZE;
		
//		System.out.println("Size: " + NODE_DB_SIZE);
//		System.out.println("Proporcion: " + PROPORTION);
		proportional_save_friends(ID_MAX_OUT_DEGREE);
		
		POSTGRESQL_DATABASE_HI5.disconnect();

	}
	public static void with_algorithm(String args[]){
		//String n = args[0];
		String n = "15";
		N = Integer.parseInt(n);
		
		ID_MAX_OUT_DEGREE = POSTGRESQL_DATABASE_HI5G.get_ID_max_out_degree();
		save_friends(ID_MAX_OUT_DEGREE);
	}
	public static void proportional_save_friends(int id){
		//ArrayList<Integer> friends = POSTGRESQL_DATABASE.get_FRIENDS_of(id);
	}
	public static void save_friends(int id){
		//Graph g = new Graph(16);
		
		nodes.put(id, id);
		
		if(COUNT >= N){
			return;
		}
		else{
			ArrayList<Edge> edges = POSTGRESQL_DATABASE_HI5G.get_FRIENDSHIPS_of(id);
			g.add_edges(edges);
			
			for(Edge e : edges){
				System.out.println(e.ID_node_1 + " " + e.ID_node_2);
				if(!queue.contains(e.ID_node_2) && !nodes.contains(e.ID_node_2)){
					queue.add(e.ID_node_2);
					COUNT++;
				}
			}
		}
		if(queue.size()>0){
			save_friends(queue.remove());
		}
		else{
			System.out.println("Problemas, no se alcanza a completar el grafo");
		}
	}
}
