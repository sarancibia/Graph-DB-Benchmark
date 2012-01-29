package data_generator.cleaner;

//import data_generator.database.POSTGRESQL_DATABASE_HI5;
import data_generator.database.POSTGRESQL_DATABASE_HI5;
import data_generator.database.POSTGRESQL_DATABASE_HI5G;

public class Cleaner {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		edge_unidirectional(args);
		//edge_bidirectional(args);
	}
	public static void edge_unidirectional(String[] args){
		POSTGRESQL_DATABASE_HI5G.connect();
		POSTGRESQL_DATABASE_HI5.connect();
		
		POSTGRESQL_DATABASE_HI5G.create_unidirectional_db();
//		int range = Integer.parseInt(args[0]);
//		int block = Integer.parseInt(args[1]);
		int range = 0;
		int block = 1000;
		
		POSTGRESQL_DATABASE_HI5G.save_properties();
		POSTGRESQL_DATABASE_HI5G.save_nodes();
		POSTGRESQL_DATABASE_HI5G.save_attributes();
		
		int mini_bloques = 1000;
		for(int i = 0; i < block; i+=mini_bloques){
			POSTGRESQL_DATABASE_HI5G.save_unidirectional_edges(range*block+i, mini_bloques);
		}
		
		POSTGRESQL_DATABASE_HI5.disconnect();	
		POSTGRESQL_DATABASE_HI5G.disconnect();
	}
	public static void edge_bidirectional(String[] args){
		POSTGRESQL_DATABASE_HI5G.connect();
		POSTGRESQL_DATABASE_HI5.connect();
		
		//POSTGRESQL_DATABASE_HI5G.create_bidirectional_db();
		int range = Integer.parseInt(args[0]);
		int block = Integer.parseInt(args[1]);
		//POSTGRESQL_DATABASE_HI5G.save_properties();
		//POSTGRESQL_DATABASE_HI5G.save_nodes();
		//POSTGRESQL_DATABASE_HI5G.save_attributes();
		int mini_bloques = 1000;
		for(int i = 0; i < block; i+=mini_bloques){
			POSTGRESQL_DATABASE_HI5G.save_bidirectional_edges(range*block+i, mini_bloques);
		}
		
		POSTGRESQL_DATABASE_HI5.disconnect();	
		POSTGRESQL_DATABASE_HI5G.disconnect();
	}


}
