package data_generator.objects;
import java.util.ArrayList;

public class Adyacencia {
	public int key;
	public ArrayList<Integer> adyacencias;
	
	public Adyacencia(int key){
		this.key = key;
		this.adyacencias = new ArrayList<Integer>();
	}
	public void add(int id_node_2){
		this.adyacencias.add(id_node_2);
	}
	public String toString(){
		String as = "";
		for(int i : adyacencias){
			as += i + " ";
		}
		return key + " -> " + as;
	}
}
