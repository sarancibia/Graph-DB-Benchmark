package data_generator.database;

import java.sql.*;
import java.util.ArrayList;
import java.util.Hashtable;

import data_generator.exporter.FILE_OUTPUT;
import data_generator.objects.Edge;
import data_generator.objects.Node;
import data_generator.objects.Relationship;
import data_generator.objects.Profile;

public class MYSQL_DATABASE {
	public static String mysql_url = "jdbc:mysql://localhost/hi5";
	public static String mysql_username = "sarancibia";
	public static String mysql_password = "sarancibia";
	
	public static Hashtable<Integer, String> property_id_nombre = new Hashtable<Integer, String>();
	public static Hashtable<String, Integer> property_nombre_id = new Hashtable<String, Integer>();
	

	public static void save_profile(Profile p){		
        try {
            DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
            Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);

            PreparedStatement s = conexion.prepareStatement("INSERT INTO Hi5_profile " +
            		"(url, nombre, sexo, edad, cumpleanos, ubicacion, ciudad_natal, estado_civil, buscando, acerca_de_mi, intereses, musica_favorita, peliculas_favoritas, programas_de_tv_favoritos, libros_favoritos, cita_favorita, idiomas) VALUES " +
            		"(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            
            s.setString(1,"http://hi5.com"+p.url);
            s.setString(2,p.name);
            s.setString(3, p.sex);
            s.setString(4,p.age);
            s.setString(5,p.birthday);
            s.setString(6,p.location);
            s.setString(7,"");//s.setString(7,p.ciudad_natal);
            s.setString(8,"");//s.setString(8,p.estado_civil);
            s.setString(9,p.looking_to);
            s.setString(10,p.about_me);
            s.setString(11,p.interests);
            s.setString(12,p.favorite_music);
            s.setString(13,p.favorite_movies);
            s.setString(14,p.favorite_tv_shows);
            s.setString(15,p.favorite_books);
            s.setString(16,p.favorite_quote);
            s.setString(17,p.languages);
                    
            try{
            	s.executeUpdate(); 
            	conexion.close();
            }catch(Exception e){
            	if(p.url != null){
	            	System.out.println("No se pudo guardar Perfil en BD: " + p.url + "\n" + e);
	            	FILE_OUTPUT.write("\t No se pudo guardar Perfil en BD: " + p.url + "\n" + e);
            	}
            	conexion.close();
            }
        }
        catch (Exception e){
        	System.out.println("No se puede crear conexión a la BD");
        	FILE_OUTPUT.write("\t No se puede crear conexión a la BD");
        }	
    }
	public static void save_friendship(String profile_a, String profile_b){
        try {
            DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
            Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);

            PreparedStatement s = conexion.prepareStatement("INSERT INTO Hi5_friends " +
            		"(profile_a, profile_b) VALUES " +
            		"(?,?)");
            
            s.setString(1,"http://hi5.com"+profile_a);
            s.setString(2,"http://hi5.com"+profile_b);            
            
            try{
            	s.executeUpdate(); 
            	conexion.close();
            }catch(Exception e){
            	System.out.println("No se puede guardar relación de amistad: " + profile_a + "-" + profile_b + "\n " + e);
            	FILE_OUTPUT.write("\t No se puede guardar relación de amistad: " + profile_a + "-" + profile_b + "\n " + e);
            	conexion.close();
            }
        }
        catch (Exception e){
        	System.out.println("No se puede crear conexión a la BD");
        	FILE_OUTPUT.write("\t No se puede crear conexión a la BD");
        }	
    }
	public static boolean exist(String node) {
		 try {
	            DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
	            Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);

	            Statement s = conexion.createStatement();
	            ResultSet rs = s.executeQuery("SELECT url FROM Hi5_profile WHERE url = 'http://hi5.com" + node + "'");

	            if (rs.next()) {
	            	conexion.close();
	            	return true;
	            }
	            else{
		            rs = s.executeQuery("SELECT url FROM Hi5_private_profile WHERE url = 'http://hi5.com" + node + "'");
		            if (rs.next()) { 
		            	conexion.close();
		            	return true;
		            }
		            else{
		            	conexion.close();
		            	return false;
		            }
	            }
	                    
	           
		 }catch(Exception e){
	        	System.out.println("No se puede crear conexión a la BD");
	        	FILE_OUTPUT.write("\t No se puede crear conexión a la BD");
	        	return false;
		 }
	}
	public static ArrayList<String> dequeue(){
		ArrayList<String> urls = new ArrayList<String>();
		
		
		 try {
            DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
            Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);

            Statement s = conexion.createStatement();
            ResultSet rs = s.executeQuery("SELECT url FROM Hi5_queue ORDER BY TIME LIMIT 10");
            

            while (rs.next()) { 
            	String url = rs.getString(1);
            	urls.add(url);
            	
            	PreparedStatement ps = conexion.prepareStatement("DELETE FROM Hi5_queue WHERE url=?");
                ps.setString(1, url);
                
                try{
                	ps.executeUpdate(); 
                }catch(Exception e){
                	if(url != null){
    	            	System.out.println("No se puede desencolar en BD: " + url + "\n" + e);
    	            	FILE_OUTPUT.write("\t No se puede desencolar en BD: " + url + "\n" + e);
                	}
                }
            }
            	
            conexion.close();
            return urls;
	                    
	           
		 }catch(Exception e){
        	System.out.println("No se puede crear conexión a la BD");
        	FILE_OUTPUT.write("\t No se puede crear conexión a la BD");
        	
        	return null;
		 }
	}
	public static boolean enqueue(String url) {
        try {
            DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
            Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);

            PreparedStatement s = conexion.prepareStatement("INSERT INTO Hi5_queue (url) VALUES (?)");
            
            s.setString(1, url);
                    
            try{
            	s.executeUpdate(); 
            	conexion.close();
            	return true;
            }catch(Exception e){
            	if(url != null){
	            	System.out.println("No se pudo encolar en BD: " + url + "\n" + e);
	            	FILE_OUTPUT.write("\t No se pudo encolar en BD: " + url + "\n" + e);
            	}
            	conexion.close();
            	return false;
            }
        }
        catch (Exception e){
        	System.out.println("No se puede crear conexión a la BD");
        	FILE_OUTPUT.write("\t No se puede crear conexión a la BD");
        	return false;
        }	
	}
	public static boolean queue_isEmpty() {
		 try {
			DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
			Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);
			
			Statement s = conexion.createStatement();
			ResultSet rs = s.executeQuery("SELECT url FROM Hi5_queue ORDER BY TIME LIMIT 1");
			
			
			if (rs.next()) { 
				conexion.close();
				return false;
			}
			else{
				conexion.close();
				return true;
			}
		                    
		           
		 }catch(Exception e){
			System.out.println("No se puede crear conexión a la BD");
			FILE_OUTPUT.write("\t No se puede crear conexión a la BD");
	        	
	        return false;
		}
	}
	public static void save_private_profile(String url) {	
        try {
            DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
            Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);

            PreparedStatement s = conexion.prepareStatement("INSERT INTO Hi5_private_profile (url) VALUES (?)");
            
            s.setString(1, "http://hi5.com"+url);
                    
            try{
            	s.executeUpdate(); 
            	conexion.close();
            }catch(Exception e){
            	if(url != null){
	            	System.out.println("No se pudo encolar en BD: " + url + "\n" + e);
	            	FILE_OUTPUT.write("\t No se pudo encolar en BD: " + url + "\n" + e);
            	}
            	conexion.close();
            }
        }
        catch (Exception e){
        	System.out.println("No se puede crear conexión a la BD");
        	FILE_OUTPUT.write("\t No se puede crear conexión a la BD");
        }	
	}
	public static ArrayList<Relationship> getFriendships(int block, int range) {
		ArrayList<Relationship> relaciones = new ArrayList<Relationship>();
		
		try {
			DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
			Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);

			Statement s = conexion.createStatement();
			
			String query = "SELECT * FROM hi520k.Hi5_friends LIMIT " + block + "," + range;
			System.out.println(query);
			
			ResultSet rs = s.executeQuery(query);
			while (rs.next()) { 
				relaciones.add(new Relationship(rs.getString(1), rs.getString(2)));
			}
		
			conexion.close();
			
			return relaciones;
           
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("No se puede crear conexión a la BD");
			FILE_OUTPUT.write("\t No se puede crear conexión a la BD");
			
			return relaciones;
		 }
	}
	public static ArrayList<Node> getNodes(int block, int range) {
		String mysql_url = "jdbc:mysql://localhost/hi520k";
		
		ArrayList<Node> nodes = new ArrayList<Node>();
		
		try {
			DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
			Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);

			Statement s = conexion.createStatement();
			
			int start = block*range;
			String query = "SELECT * FROM Node LIMIT " + start + "," + range;
			
			ResultSet rs = s.executeQuery(query);
			while (rs.next()) { 
				nodes.add(new Node(rs.getInt(1), rs.getString(2), rs.getInt(3), rs.getInt(4), rs.getInt(5)));
			}
		
			conexion.close();
			
			return nodes;
           
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("No se puede crear conexión a la BD");
			FILE_OUTPUT.write("\t No se puede crear conexión a la BD");
			
			return nodes;
		 }
	}
	public static ArrayList<Edge> getEdges(int block, int range) {
		String mysql_url = "jdbc:mysql://localhost/hi520k";
		
		ArrayList<Edge> edges = new ArrayList<Edge>();
		
		try {
			DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
			Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);

			Statement s = conexion.createStatement();
			
			int start = block*range;
			String query = "SELECT * FROM Edge LIMIT " + start + "," + range;
			
			ResultSet rs = s.executeQuery(query);
			while (rs.next()) { 
				edges.add(new Edge(rs.getInt(1), rs.getInt(2), rs.getInt(3), rs.getInt(4)));
			}
		
			conexion.close();
			
			return edges;
           
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("No se puede crear conexión a la BD");
			FILE_OUTPUT.write("\t No se puede crear conexión a la BD");
			
			return edges;
		 }
	}
	public static void load_property() {
		 try {
			//System.out.println(query);
			DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
			Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);
			
			Statement s = conexion.createStatement();
			ResultSet r = s.executeQuery("SELECT * FROM hi5graph.Property");
		
			try {
				while (r.next()){
					property_id_nombre.put(r.getInt(1), r.getString(2));
					property_nombre_id.put(r.getString(2), r.getInt(1));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			conexion.close();
  			           
		 }catch(Exception e){
			 e.printStackTrace();
		 }
	}
	public static void save_edge(int idA, Integer property, int idB) {
        try {
            DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
            Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);

            PreparedStatement s = conexion.prepareStatement("INSERT INTO hi5graph.Edge (ID_node_1, ID_property, ID_node_2) VALUES (?,?,?);");
            s.setInt(1, idA);
            s.setInt(2, (int)property);
            s.setInt(3, idB);
            try{
            	s.executeUpdate(); 
            }catch(Exception e){
            	e.printStackTrace();
            }
            
            s = conexion.prepareStatement("UPDATE hi5graph.Property SET counter = counter+1 WHERE ID=?");
            s.setInt(1, (int)property);
            try{
            	s.executeUpdate(); 
            }catch(Exception e){
            	e.printStackTrace();
            }
            
            s = conexion.prepareStatement("UPDATE hi5graph.Node SET in_degree = in_degree+1 WHERE ID=?");
            s.setInt(1, idB);
            try{
            	s.executeUpdate(); 
            }catch(Exception e){
            	e.printStackTrace();
            }
            
            s = conexion.prepareStatement("UPDATE hi5graph.Node SET out_degree = out_degree+1 WHERE ID=?");
            s.setInt(1, idA);
            try{
            	s.executeUpdate(); 
            }catch(Exception e){
            	e.printStackTrace();
            }
            
            conexion.close();
        }
        catch (Exception e){
        	e.printStackTrace();
        	System.out.println("No se puede crear conexión a la BD");
        	FILE_OUTPUT.write("\t No se puede crear conexión a la BD");
        }	
	}

	public static void save(Relationship r){
		int id_a=0, id_b=0;
		
		try {
			DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
			Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);
			PreparedStatement s = conexion.prepareStatement("INSERT INTO hi5graph.Node (nombre) VALUES (?)");
			
			s.setString(1, r.profile_a);     
			try{
				s.executeUpdate(); 
				//obtener ultimo id
				Statement st = conexion.createStatement();
				ResultSet rs = st.executeQuery("SELECT LAST_INSERT_ID()");
				if(rs.next()){
					id_a = rs.getInt(1);
				}
			}catch(Exception e){
				//buscar
				Statement st = conexion.createStatement();
				ResultSet rs = st.executeQuery("SELECT ID FROM hi5graph.Node WHERE nombre = '" + r.profile_a + "'");
				if(rs.next()){
					id_a = rs.getInt(1);
				}
			}
			
			s.setString(1, r.profile_b);
			try{
				s.executeUpdate(); 
				//obtener ultimo id
				Statement st = conexion.createStatement();
				ResultSet rs = st.executeQuery("SELECT LAST_INSERT_ID()");
				if(rs.next()){
					id_b = rs.getInt(1);
				}
			}catch(Exception e){
				//buscar
				Statement st = conexion.createStatement();
				ResultSet rs = st.executeQuery("SELECT ID FROM hi5graph.Node WHERE nombre = '" + r.profile_b + "'");
				if(rs.next()){
					id_b = rs.getInt(1);
				}
			}
			conexion.close();

			Profile pa = getProfile(r.profile_a);
			Profile pb = getProfile(r.profile_b);

			save_edge(id_a,  property_nombre_id.get("Relationship"), id_b);
			
			update_node(id_a, pa);
			update_node(id_b, pb);
	    }
        catch (Exception e){
        	e.printStackTrace();
        }	
	}
	private static void update_node(int id, Profile p) {
		if(p != null){
			save_property(id, property_nombre_id.get("url"), p.url);
			save_property(id, property_nombre_id.get("name"), p.name);
			save_property(id, property_nombre_id.get("sex"), p.sex);
			save_property(id, property_nombre_id.get("age"), p.age);
			save_property(id, property_nombre_id.get("birthday"), p.birthday);
			save_property(id, property_nombre_id.get("location"), p.location);
			save_property(id, property_nombre_id.get("looking_to"), p.looking_to);
			save_property(id, property_nombre_id.get("about_me"), p.about_me);
			save_property(id, property_nombre_id.get("interests"), p.interests);
			save_property(id, property_nombre_id.get("favorite_music"), p.favorite_music);
			save_property(id, property_nombre_id.get("favorite_movies"), p.favorite_movies);
			save_property(id, property_nombre_id.get("favorite_tv_shows"), p.favorite_tv_shows);
			save_property(id, property_nombre_id.get("favorite_books"), p.favorite_books);
			save_property(id, property_nombre_id.get("favorite_quote"), p.favorite_quote);
			save_property(id, property_nombre_id.get("languages"), p.languages);
		}
	}
	private static void save_property(int id_node, int id_property, String value) {
		value = value.trim();
		
		if(value != null && !value.equals("")){
			try{
				DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
				Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);
				
				PreparedStatement s1 = conexion.prepareStatement("INSERT INTO hi5graph.Attribute (ID_node, ID_property, value) VALUES (?,?,?)");
				s1.setInt(1, id_node);
				
				PreparedStatement s2 = conexion.prepareStatement("UPDATE hi5graph.Property SET counter = counter+1 WHERE ID=?");
				s2.setInt(1, id_property);
				
				PreparedStatement s3 = conexion.prepareStatement("UPDATE hi5graph.Node SET nro_attributes = nro_attributes+1 WHERE ID=?");
				s3.setInt(1, id_node);
				
	
				s1.setInt(2, id_property);
				s1.setString(3, value);
				try{
					s1.executeUpdate(); 
					s2.executeUpdate();
					s3.executeUpdate();
				}catch(Exception e){}
				
				conexion.close();
			} catch(Exception e){}
		}
	}
	private static Profile getProfile(String url) {
		try {
			DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
            Connection conexion = DriverManager.getConnection (mysql_url, mysql_username, mysql_password);

            Statement s = conexion.createStatement();
            ResultSet rs = s.executeQuery("SELECT * FROM Hi5_profile WHERE url = '" + url + "'");

            if (rs.next()) {
            	Profile p = new Profile();
            	
            	p.url = rs.getString("url");
            	p.name = rs.getString("nombre");
            	p.sex = rs.getString("sexo");
            	p.age = rs.getString("edad");
            	p.birthday = rs.getString("cumpleanos");
            	p.location = rs.getString("ubicacion");
            	p.looking_to = rs.getString("buscando");
            	p.about_me = rs.getString("acerca_de_mi");
            	p.interests = rs.getString("intereses");
            	p.favorite_music = rs.getString("musica_favorita");
            	p.favorite_movies = rs.getString("peliculas_favoritas");
            	p.favorite_tv_shows = rs.getString("programas_de_tv_favoritos");
            	p.favorite_books = rs.getString("libros_favoritos");
            	p.favorite_quote =  rs.getString("cita_favorita");
            	p.languages = rs.getString("idiomas");
            	
            	p.fix();
            	
            	conexion.close();
            	return p;
            }
            else{
            	conexion.close();
            	return null;
            }
		 }catch(Exception e){
			 e.printStackTrace();
			 return null;
		 }
	}
}