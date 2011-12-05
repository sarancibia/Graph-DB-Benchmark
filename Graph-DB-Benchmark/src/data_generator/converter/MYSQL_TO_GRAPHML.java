package data_generator.converter;

import java.util.ArrayList;
import data_generator.database.MYSQL_DATABASE;
import data_generator.exporter.GRAPHML_EXPORT;
import data_generator.objects.Relationship;

public class MYSQL_TO_GRAPHML {
	public static void main(String[] args) {
		ArrayList<Relationship> relaciones = MYSQL_DATABASE.getFriendships(0, 10000);
		GRAPHML_EXPORT.save(relaciones);
	}
}
