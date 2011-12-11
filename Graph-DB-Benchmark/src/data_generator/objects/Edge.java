package data_generator.objects;

public class Edge {
	public int ID;
	public int ID_node_1;
	public int ID_property;
	public int ID_node_2;
	
	public Edge(int ID, int ID_node_1, int ID_property, int ID_node_2){
		this.ID = ID;
		this.ID_node_1 = ID_node_1;
		this.ID_property = ID_property;
		this.ID_node_2 = ID_node_2;
	}
}
