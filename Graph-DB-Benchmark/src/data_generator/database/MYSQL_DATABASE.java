package data_generator.database;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.ArrayList;

import com.sparsity.dex.gdb.AttributeKind;
import com.sparsity.dex.gdb.DataType;
import com.sparsity.dex.gdb.Dex;
import com.sparsity.dex.gdb.DexConfig;

import data_generator.exporter.FILE_OUTPUT;
import data_generator.objects.Amistad;
import data_generator.objects.Profile;

public class MYSQL_DATABASE {
//	public static String mysql_url = "jdbc:mysql://localhost/Bench_Graph";
//	public static String mysql_usuario = "root";
//	public static String mysql_contrasena = "root";
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
            s.setString(2,p.nombre);
            s.setString(3, p.sexo);
            s.setString(4,p.edad);
            s.setString(5,p.cumpleanos);
            s.setString(6,p.ubicacion);
            s.setString(7,p.ciudad_natal);
            s.setString(8,p.estado_civil);
            s.setString(9,p.buscando);
            s.setString(10,p.acerca_de_mi);
            s.setString(11,p.intereses);
            s.setString(12,p.musica_favorita);
            s.setString(13,p.peliculas_favoritas);
            s.setString(14,p.programas_de_tv_favoritos);
            s.setString(15,p.libros_favoritos);
            s.setString(16,p.cita_favorita);
            s.setString(17,p.idiomas);
                    
            try{
            	s.executeUpdate(); 
            	conexion.close();
            }catch(Exception e){
            	if(p.url != null){
	            	System.out.println("No se pudo guardar Perfil en BD: " + p.url + "\n" + e);
	            	FILE_OUTPUT.escribir("\t No se pudo guardar Perfil en BD: " + p.url + "\n" + e);
            	}
            	conexion.close();
            }
        }
        catch (Exception e){
        	System.out.println("No se puede crear conexión a la BD");
        	FILE_OUTPUT.escribir("\t No se puede crear conexión a la BD");
        }	
    }
	public static void save_friends(String profile_a, String profile_b){
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
            	FILE_OUTPUT.escribir("\t No se puede guardar relación de amistad: " + profile_a + "-" + profile_b + "\n " + e);
            	conexion.close();
            }
        }
        catch (Exception e){
        	System.out.println("No se puede crear conexión a la BD");
        	FILE_OUTPUT.escribir("\t No se puede crear conexión a la BD");
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
	        	FILE_OUTPUT.escribir("\t No se puede crear conexión a la BD");
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
    	            	FILE_OUTPUT.escribir("\t No se puede desencolar en BD: " + url + "\n" + e);
                	}
                }
            }
            	
            conexion.close();
            return urls;
	                    
	           
		 }catch(Exception e){
        	System.out.println("No se puede crear conexión a la BD");
        	FILE_OUTPUT.escribir("\t No se puede crear conexión a la BD");
        	
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
	            	FILE_OUTPUT.escribir("\t No se pudo encolar en BD: " + url + "\n" + e);
            	}
            	conexion.close();
            	return false;
            }
        }
        catch (Exception e){
        	System.out.println("No se puede crear conexión a la BD");
        	FILE_OUTPUT.escribir("\t No se puede crear conexión a la BD");
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
			FILE_OUTPUT.escribir("\t No se puede crear conexión a la BD");
	        	
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
	            	FILE_OUTPUT.escribir("\t No se pudo encolar en BD: " + url + "\n" + e);
            	}
            	conexion.close();
            }
        }
        catch (Exception e){
        	System.out.println("No se puede crear conexión a la BD");
        	FILE_OUTPUT.escribir("\t No se puede crear conexión a la BD");
        }	
	}
	public static int num_nodos(){
		 try {
	            DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
	            Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

	            Statement s = conexion.createStatement();
	            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM Hi5_profile");


		        if (rs.next()){
		        	int a = rs.getInt(1);
		        	
		        	ResultSet rs2 = s.executeQuery("SELECT COUNT(*) FROM Hi5_private_profile");
		        	
		        	if(rs2.next()){
		        		conexion.close();
		        		return a + rs2.getInt(1);
		        	}
		        	conexion.close();
		        	return rs.getInt(1);
		        }
		        else
		        	return -1;
		 }catch(Exception e){
			 return -1;
		 }
	}
	public static int num_relaciones(String url){
		return 0;
	}
	public static int num_aristas(){
		try {
            DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
            Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

            Statement s = conexion.createStatement();
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM Hi5_friends");


	        if (rs.next()) {
	        	conexion.close();
	        	return rs.getInt(1);
	        }
	        else{
	        	conexion.close();
	        	return -1;
	        }
	 }catch(Exception e){
		 return -1;
	 }
	}
	public static int num_cola(){
		try {
            DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
            Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

            Statement s = conexion.createStatement();
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM Hi5_queue");


	        if (rs.next()){
	        	conexion.close();
	        	return rs.getInt(1);
	        }
	        else{
	        	conexion.close();
	        	return -1;
	        }
	 }catch(Exception e){
		 return -1;
	 }
		
	}
	public static int num_perfil_privado(){
		 try {
	            DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
	            Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

	            Statement s = conexion.createStatement();
	            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM Hi5_private_profile");


		        if (rs.next()){
		        	conexion.close();
		        	return rs.getInt(1);
		        }
		        else{
		        	conexion.close();
		        	return -1;
		        }
		 }catch(Exception e){
			 return -1;
		 }
	}
	public static int num_perfil_publicos(){
		 try {
	            DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
	            Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

	            Statement s = conexion.createStatement();
	            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM Hi5_profile");


		        if (rs.next()){
		        	conexion.close();
		        	return rs.getInt(1);
		        }
		        else{
		        	conexion.close();
		        	return -1;
		        }
		 }catch(Exception e){
			 return -1;
		 }
	}
	public static int relaciones_to_DEX(int i, int rango) {
		int filas = 0;
		try {
			DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
			Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

			Statement s = conexion.createStatement();
			
			int inicio = i*rango;
			String query = "SELECT * FROM Hi5_friends ORDER BY profile_a, profile_b LIMIT " + inicio + "," + rango;
			
			System.out.println(query);
			
			
			try{
				ResultSet rs = s.executeQuery(query);

				DEX_DATABASE.open(DEX_DATABASE.DB_NAME);
				while (rs.next()) { 
					
					Amistad relacion = new Amistad(rs.getString(1), rs.getString(2));
					DEX_DATABASE.amistad_guardar(relacion);
					filas++;
					
				}
				DEX_DATABASE.close();
				conexion.close();
			}catch(Exception e){
				conexion.close();
			}
			
			return filas;
           
		}catch(Exception e){
			System.out.println("Problema!");
			e.printStackTrace();
			return 1;
		}
	}
	public static ArrayList<Amistad> getRelaciones(int bloque, int rango) {
		ArrayList<Amistad> relaciones = new ArrayList<Amistad>();
		
		try {
			DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
			Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

			Statement s = conexion.createStatement();
			
			int inicio = bloque*rango;
			
			String query = "SELECT * FROM Hi5_friends ORDER BY profile_a, profile_b LIMIT " + inicio + "," + rango;
			//System.out.println(query);
			
			ResultSet rs = s.executeQuery(query);
			while (rs.next()) { 
				relaciones.add(new Amistad(rs.getString(1), rs.getString(2)));
			}
		
			conexion.close();
			
			return relaciones;
           
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("No se puede crear conexión a la BD");
			FILE_OUTPUT.escribir("\t No se puede crear conexión a la BD");
			
			return relaciones;
		 }
	}
	public static void emulaGrafo() {
		int rango = 1000000;
		int i = 0;
		
		//int filas = 0;
		try {
			DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
			Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

			Statement s = conexion.createStatement();
			
			int inicio = i*rango;
			String query = "SELECT * FROM Hi5_friends ORDER BY profile_a, profile_b LIMIT " + inicio + "," + rango;
			ResultSet rs = s.executeQuery(query);
           
			DexConfig conf = new DexConfig();
			conf.setLicense(DEX_DATABASE.LICENCE);
			
			try {
				Dex dex = new Dex(conf);
				DEX_DATABASE.db = dex.create(DEX_DATABASE.DB_NAME, DEX_DATABASE.DB_NAME);
			    DEX_DATABASE.s = DEX_DATABASE.db.newSession();
			    DEX_DATABASE.g = DEX_DATABASE.s.getGraph();
				   
				DEX_DATABASE.OID_PERSONA = DEX_DATABASE.g.newNodeType(DEX_DATABASE.PERSONA);
				DEX_DATABASE.OID_PERSONA_ID = DEX_DATABASE.g.newAttribute(DEX_DATABASE.OID_PERSONA, DEX_DATABASE.PERSONA_ID, DataType.String, AttributeKind.Basic);
				DEX_DATABASE.OID_AMISTAD = DEX_DATABASE.g.newEdgeType(DEX_DATABASE.AMISTAD, true, true);
					
				DEX_DATABASE.s.commit();
				DEX_DATABASE.db.close();
			    dex.close(); 	
				
			    
			    dex = new Dex(conf);
			    DEX_DATABASE.db = dex.open(DEX_DATABASE.DB_NAME, false);
				
			    while (rs.next()) { 
			    	//Amistad relacion = new Amistad(rs.getString(1), rs.getString(2));
						DEX_DATABASE.s = DEX_DATABASE.db.newSession();;
						DEX_DATABASE.g = DEX_DATABASE.s.getGraph();

						long a = DEX_DATABASE.g.newNode(DEX_DATABASE.OID_PERSONA);
						long b = DEX_DATABASE.g.newNode(DEX_DATABASE.OID_PERSONA);

						DEX_DATABASE.g.newEdge(DEX_DATABASE.OID_AMISTAD, a, b);

						DEX_DATABASE.s.commit();
						DEX_DATABASE.s.close();
				}
			    
				DEX_DATABASE.db.close();
				dex.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			conexion.close();
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("No se puede crear conexión a la BD");
			FILE_OUTPUT.escribir("\t No se puede crear conexión a la BD");
		}
	}
	public static void MySQLToGraphML() {
				
		try {
			DriverManager.registerDriver(new org.gjt.mm.mysql.Driver());
			Connection conexion = DriverManager.getConnection (mysql_url, mysql_usuario, mysql_contrasena);

			Statement s = conexion.createStatement();
//			int contador, bloque = 0, rango = 1000000;
//			do{
				String query = 	"SELECT DISTINCT profile_a, profile_b " +
								"FROM Hi5_friends " +
								"WHERE (profile_b, profile_a) NOT IN " +
								"(SELECT profile_a, profile_b FROM Hi5_friends)";
//				String query = 	"SELECT DISTINCT profile_a, profile_b " +
//				"FROM Hi5_friends " +
//				"WHERE (profile_b, profile_a) NOT IN " +
//				"(SELECT profile_a, profile_b FROM Hi5_friends) LIMIT " + rango*(bloque++) + ","+rango;
				System.out.println(query);			
				ResultSet rs = s.executeQuery(query);
				
//				contador = 0;
				while (rs.next()) { 
					System.out.println(new Amistad(rs.getString(1), rs.getString(2)));
//					contador++;
				}
//			}while(contador > 0);
		
			conexion.close();
           
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("No se puede crear conexión a la BD");
			FILE_OUTPUT.escribir("\t No se puede crear conexión a la BD");
		 }
		
	}
}