package data_generator.exporter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import data_generator.objects.Edge;


public class DOT_EXPORT {
    private static FileWriter file = null;
    private static PrintWriter pw = null;
    
	public static void open(String name){
        try{
            file = new FileWriter(name);
            pw = new PrintWriter(file);
            
            pw.println("graph Test{");


        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	public static void close(){
        try {
        	pw.println("}");
        	pw.close();
			file.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void save_undirected_edges(ArrayList<Edge> edges) {
		for(Edge edge : edges){
			//pw.println("\t " + edge.ID_node_1 + " -- " + edge.ID_node_2 + " [label = \"" + edge.ID + "\"];");
			pw.println("\t " + edge.ID_node_1 + " -- " + edge.ID_node_2 + ";");
		}
	}
}
