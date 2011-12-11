package data_generator.converter;

import java.util.ArrayList;

import data_generator.database.MYSQL_DATABASE;
import data_generator.exporter.GRAPHML_EXPORT;
import data_generator.objects.Node;

public class MYSQLGRAPHFORMAT_TO_GRAPHML {
	static int BLOCK = 1000000;
	
	public static void main(String[] args) {
		GRAPHML_EXPORT.open();
		
		int i;
		ArrayList<Node> nodes = new ArrayList<Node>();
		
		i=0;
		do{
			GRAPHML_EXPORT.save_nodes(MYSQL_DATABASE.getNodes(i++, BLOCK));
		}while(nodes.size() == BLOCK);
		
		i=0;
		do{
			GRAPHML_EXPORT.save_edges(MYSQL_DATABASE.getEdges(i++, BLOCK));
		}while(nodes.size() == BLOCK);		
		
		
		GRAPHML_EXPORT.close();

	}

}
