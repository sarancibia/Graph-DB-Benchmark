package data_generator.exporter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import data_generator.objects.Edge;
import data_generator.objects.Node;


public class GRAPHML_EXPORT {
    private static FileWriter file = null;
    private static PrintWriter pw = null;
    
	public static void open(){
        try{
            file = new FileWriter("Hi5.graphml");
            pw = new PrintWriter(file);

            pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            pw.println("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"");
            pw.println("\txmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
            pw.println("\txsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns");
            pw.println("\thttp://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">");
            pw.println("\t<graph id=\"G\" edgedefault=\"directed\">");

        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	public static void close(){
        try {
        	pw.println("\t</graph>");
        	pw.println("</graphml>");
        	
        	pw.close();
			file.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void save_nodes(ArrayList<Node> nodes) {
		for(Node node : nodes){
        	pw.println("\t\t<node id=\""+node.ID+"\"/>");
        }
	}
	public static void save_edges(ArrayList<Edge> edges) {
		for(Edge edge : edges){
			pw.println("<edge id=\"e"+ edge.ID +"\" source=\"" + edge.ID_node_1 + "\" target=\"" + edge.ID_node_2 + "\"/>");
		}
		
	}


}
