package data_generator.objects;

public class Node {
	public int ID;
	public String name;
	public int nro_attributes;
	public int in_degree;
	public int out_degree;
	
	public Node(int ID, String name, int nro_attributes, int in_degree, int out_degree) {
		this.ID = ID;
		this.name = name;
		this.nro_attributes = nro_attributes;
		this.in_degree = in_degree;
		this.out_degree = out_degree;
	}
}
