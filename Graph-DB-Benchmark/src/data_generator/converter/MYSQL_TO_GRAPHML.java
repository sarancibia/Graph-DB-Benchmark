package data_generator.converter;

import java.util.ArrayList;
import data_generator.database.MYSQL_DATABASE;
import data_generator.exporter.GRAPHML_EXPORT;
import data_generator.objects.Amistad;

public class MYSQL_TO_GRAPHML {
	public static void main(String[] args) {
		//DB.MySQLToGraphML();
		ArrayList<Amistad> relaciones = MYSQL_DATABASE.getRelaciones(0, 10000);
		GRAPHML_EXPORT.save(relaciones);
	}
}
