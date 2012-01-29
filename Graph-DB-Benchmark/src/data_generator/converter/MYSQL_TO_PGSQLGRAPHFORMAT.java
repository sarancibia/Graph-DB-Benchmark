package data_generator.converter;

import java.util.ArrayList;

import data_generator.database.MYSQL_DATABASE;
import data_generator.database.POSTGRESQL_DATABASE_HI5;
import data_generator.objects.Profile;
import data_generator.objects.Relationship;

public class MYSQL_TO_PGSQLGRAPHFORMAT {
	static int RANGE;
	static int BLOCK;//200000

	public static void main(String[] args) {
//		POSTGRESQL_DATABASE.connect();
//		POSTGRESQL_DATABASE.create_db();
//		POSTGRESQL_DATABASE.disconnect();
		
		
		RANGE = Integer.parseInt(args[0]);
		BLOCK = Integer.parseInt(args[1]);
		
		
		POSTGRESQL_DATABASE_HI5.connect();
		POSTGRESQL_DATABASE_HI5.load_properties();
		
		//save_edges(RANGE, BLOCK);
		save_nodes(RANGE, BLOCK);//20000000
    	POSTGRESQL_DATABASE_HI5.disconnect();
	}


	private static void save_nodes(int range, int block) {
		
		int mini_bloques = 1000;
		for(int i = 0; i < block; i+=mini_bloques){
			ArrayList<Profile> public_profiles = MYSQL_DATABASE.getPublicProfile(range*block+i, mini_bloques);
			POSTGRESQL_DATABASE_HI5.save_nodes(public_profiles);

		}
		
//		i = 0;
//		ArrayList<Profile> private_profiles;
//		do{
//			private_profiles = MYSQL_DATABASE.getPrivateProfile(i++, range);
//			POSTGRESQL_DATABASE.save_nodes(private_profiles);
//		}while(private_profiles.size() > 0);
	}
	public static void save_edges(int range, int block){
		int mini_bloques = 10000;
		for(int i = 0; i < block; i+=mini_bloques){
			ArrayList<Relationship> relations = MYSQL_DATABASE.getFriendships(range*block+i, mini_bloques);
			POSTGRESQL_DATABASE_HI5.save_edges_r(relations);
		}
	}
}
