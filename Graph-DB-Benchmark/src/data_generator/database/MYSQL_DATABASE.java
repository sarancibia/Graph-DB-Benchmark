package data_generator.database;

import java.sql.*;
import java.util.ArrayList;

import data_generator.exporter.FILE_OUTPUT;
import data_generator.objects.Relationship;
import data_generator.objects.Profile;

public class MYSQL_DATABASE {
	public static String mysql_url = "jdbc:mysql://localhost/hi5";
	public static String mysql_usuario = "sarancibia";
	public static String mysql_contrasena = "sarancibia";
	

	public static void save_profile(Profile p){		
        try {
            DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
            Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

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
            Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

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
	public static boolean exist(String nodo) {
		 try {
	            DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
	            Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

	            Statement s = conexion.createStatement();
	            ResultSet rs = s.executeQuery("SELECT url FROM Hi5_profile WHERE url = 'http://hi5.com" + nodo + "'");

	            if (rs.next()) {
	            	conexion.close();
	            	return true;
	            }
	            else{
		            rs = s.executeQuery("SELECT url FROM Hi5_private_profile WHERE url = 'http://hi5.com" + nodo + "'");
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
            Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

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
            Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

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
			Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);
			
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
            Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

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
	public static ArrayList<Relationship> getFriendships(int bloque, int rango) {
		ArrayList<Relationship> relaciones = new ArrayList<Relationship>();
		
		try {
			DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
			Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

			Statement s = conexion.createStatement();
			
			int inicio = bloque*rango;
			
			String query = "SELECT * FROM Hi5_friends ORDER BY profile_a, profile_b LIMIT " + inicio + "," + rango;
			//System.out.println(query);
			
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
}