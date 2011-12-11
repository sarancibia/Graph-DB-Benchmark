package data_generator.converter;
import java.util.ArrayList;
import data_generator.database.MYSQL_DATABASE;
import data_generator.objects.Relationship;


public class MYSQL_TO_MYSQLGRAPHFORMAT {
	static int BLOCK = 1000000;

	public static void main(String[] args) {
		MYSQL_DATABASE.load_property();
		save_nodes(20000000);
	}

	private static void save_nodes(int n){
		int range = BLOCK;
		
		for(int block = 0; block < n; block+=range){
			ArrayList<Relationship> rs = MYSQL_DATABASE.getFriendships(block, range);
			
			for(Relationship r : rs){
				MYSQL_DATABASE.save(r);
			}
		}
	}
}
