package data_generator.exporter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;

import data_generator.objects.Relationship;


public class GRAPHML_EXPORT {
    private static FileWriter fichero = null;
    private static PrintWriter pw = null;
    
	public static void save(ArrayList<Relationship> relaciones){

        try
        {
            fichero = new FileWriter("Hi5.graphml");
            pw = new PrintWriter(fichero);

            pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            pw.println("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"");
            pw.println("\txmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
            pw.println("\txsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns");
            pw.println("\thttp://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">");
            pw.println("\t<graph id=\"G\" edgedefault=\"undirected\">");
            
            ArrayList<String> ids = get_ids(relaciones);
            save_nodes(ids);
            save_edges(relaciones);
            
            pw.println("\t</graph>");
            pw.println("</graphml>");
            


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
           try {
           if (null != fichero)
              fichero.close();
           } catch (Exception e2) {
              e2.printStackTrace();
           }
        }
	}
	private static void save_nodes(ArrayList<String> ids) {
		for(String id : ids){
        	pw.println("\t\t<node id=\""+id+"\"/>");
        }
	}
	private static void save_edges(ArrayList<Relationship> relaciones) {
		int i = 0;
		for(Relationship relacion : relaciones){
			pw.println("<edge id=\"e"+ i++ +"\" source=\"" + relacion.profile_a + "\" target=\"" + relacion.profile_b + "\"/>");
		}
		
	}
	private static ArrayList<String> get_ids(ArrayList<Relationship> relaciones) {
		ArrayList<String> ids = new ArrayList<String>();
		
		for(Relationship relacion : relaciones){
        	if(!ids.contains(relacion.profile_a)){
        		ids.add(relacion.profile_a);
        	}
        	if(!ids.contains(relacion.profile_b)){
        		ids.add(relacion.profile_b);
        	}
        }
		return ids;
	}

}
