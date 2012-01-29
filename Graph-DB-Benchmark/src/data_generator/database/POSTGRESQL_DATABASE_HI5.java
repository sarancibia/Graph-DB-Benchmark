package data_generator.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import data_generator.objects.Edge;
import data_generator.objects.Profile;
import data_generator.objects.Relationship;

public class POSTGRESQL_DATABASE_HI5 {

    public static String driver = "org.postgresql.Driver";
    public static String connectString = "jdbc:postgresql://localhost:5432/hi5";
    public static String user = "sarancibia";
    public static String password = "sa2335";
    public static Connection conn;
    public static Statement stmt;
    public static Boolean connected = false;
    
	public static Hashtable<Integer, String> property_id_nombre = new Hashtable<Integer, String>();
	public static Hashtable<String, Integer> property_nombre_id = new Hashtable<String, Integer>();
	public static Hashtable<String, Integer> url_id = new Hashtable<String, Integer>();
	
    public static boolean connect(){
	    try{
	    	Class.forName(driver);
	    	conn = DriverManager.getConnection(connectString, user , password);
	    	stmt = conn.createStatement(); 
			System.out.println("Connected to the Database");
			return true;
	    }
    	catch ( Exception e ){
	    	System.out.println("dblayer.contructor error: " + e.getMessage());
	    	e.printStackTrace();
	    	return false;
	    }
    }
	public static void disconnect(){
		try{
	    	stmt.close(); 
	  	   	conn.close();
		}
		catch(Exception e){
			e.fillInStackTrace();
		}

	}
    public static void create_db(){
    	try{
    		Statement s = conn.createStatement();
    		s.execute("CREATE TABLE node"+
    				"(" +
    				"  id serial NOT NULL," +
    				"  nombre character varying(500) NOT NULL," +
    				"  nro_attributes integer NOT NULL DEFAULT 0," +
    				"  in_degree integer NOT NULL DEFAULT 0," +
    				"  out_degree integer NOT NULL DEFAULT 0," +
    				"  CONSTRAINT node_pkey PRIMARY KEY (id)," +
    				"  CONSTRAINT node_nombre_key UNIQUE (nombre)" +
    				")" +
    				"WITH (" +
    				"  OIDS=FALSE" +
    				");" +
    				"ALTER TABLE node OWNER TO sarancibia;" +
    				
    				"CREATE INDEX hash_node" +
    				"  ON node" +
    				"  USING hash" +
    				"  (nombre);" +
    				
    				"CREATE TABLE property " +
    				"(" +
    				"id serial NOT NULL, " +
    				"nombre character varying(500) NOT NULL, " +
    				"counter integer NOT NULL DEFAULT 0, " +
    				"CONSTRAINT property_pkey PRIMARY KEY (id), " +
    				"CONSTRAINT property_nombre_key UNIQUE (nombre) " +
    				") " +
    				"WITH ( " +
    				"OIDS=FALSE " +
    				"); " +
    				"ALTER TABLE property OWNER TO sarancibia; " +
    				
    				"CREATE TABLE attribute " +
    				"( " +
    				"id_node integer NOT NULL, " +
    				"id_property integer NOT NULL, " +
    				"\"value\" text NOT NULL, " +
    				"CONSTRAINT attribute_pkey PRIMARY KEY (id_node, id_property) " +
    				") " +
    				"WITH ( " +
    				"OIDS=FALSE " +
    				"); " +
    				"ALTER TABLE attribute OWNER TO sarancibia; " +
    				
    				"CREATE TABLE edge " +
    				"( " +
    				"id serial NOT NULL, " +
    				"id_node_1 integer NOT NULL, " +
    				"id_property integer NOT NULL, " +
    				"id_node_2 integer NOT NULL, " +
    				"CONSTRAINT edge_pkey PRIMARY KEY (id) " +
    				")WITH ( " +
    				"OIDS=FALSE " +
    				"); " +
    				"ALTER TABLE edge OWNER TO sarancibia; " +
    				
    				"CREATE OR REPLACE RULE insert_new_edge AS ON INSERT TO edge DO ( " +
    				"UPDATE node SET out_degree = node.out_degree + 1 WHERE node.id = new.id_node_1; " +
    				"UPDATE node SET in_degree = node.in_degree + 1 WHERE node.id = new.id_node_2; " +
    				"UPDATE property SET counter = counter+1 WHERE ID=new.id_property; " +
    				"); " +
    				
    				"CREATE OR REPLACE RULE insert_new_attribute AS ON INSERT TO attribute DO ( " +
    				"UPDATE property SET counter = counter+1 WHERE ID=new.id_property; " +
    				"UPDATE node SET nro_attributes = nro_attributes+1 WHERE ID=new.id_node; " +
    				");");
    	}catch(Exception e){
    		System.out.println(e.getMessage());
    		e.printStackTrace();
    	}
    }
	public static boolean is_connected(){
   		if(connected) 
   			return true;
    	else 
    		return false;
    }
	public static void load_properties() {
		try {
			Statement s = conn.createStatement();
			ResultSet r = s.executeQuery("SELECT * FROM property");
			while (r.next()){
				property_id_nombre.put(r.getInt(1), r.getString(2));
				property_nombre_id.put(r.getString(2), r.getInt(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if(property_id_nombre.size() == 0){
			save_property("Relationship");
			save_property("url");
			save_property("name");
			save_property("age");
			save_property("sex");
			save_property("birthday");
			save_property("location");
			save_property("looking_to");
			save_property("languages");
			save_property("about_me");
			save_property("interests");
			save_property("favorite_music");
			save_property("favorite_movies");
			save_property("favorite_tv_shows");
			save_property("favorite_books");
			save_property("favorite_quote");
			
			try {
				Statement s = conn.createStatement();
				ResultSet r = s.executeQuery("SELECT * FROM property");
				while (r.next()){
					property_id_nombre.put(r.getInt(1), r.getString(2));
					property_nombre_id.put(r.getString(2), r.getInt(1));
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}
	private static void save_property(String value) {
		try{
			PreparedStatement ps = conn.prepareStatement("INSERT INTO property (nombre) VALUES (?)");
			ps.setString(1, value);
		
			ps.execute();
			ps.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}		
	}
	public static void save_nodes(ArrayList<Profile> profiles) {
		for(Profile p : profiles){
			save_node(p);
		}
	}
	private static void save_node(Profile p) {
		int id = 0;
		try{
			PreparedStatement ps = conn.prepareStatement("SELECT id FROM node WHERE nombre = ?");
			ps.setString(1, p.url);
			ResultSet r = ps.executeQuery();
			
			if(r.next()){
				id = r.getInt(1);
				
				save_attribute(id, property_nombre_id.get("url"), p.url);
				save_attribute(id, property_nombre_id.get("name"), p.name);
				save_attribute(id, property_nombre_id.get("sex"), p.sex);
				save_attribute(id, property_nombre_id.get("age"), p.age);
				save_attribute(id, property_nombre_id.get("birthday"), p.birthday);
				save_attribute(id, property_nombre_id.get("location"), p.location);
				save_attribute(id, property_nombre_id.get("looking_to"), p.looking_to);
				save_attribute(id, property_nombre_id.get("about_me"), p.about_me);
				save_attribute(id, property_nombre_id.get("interests"), p.interests);
				save_attribute(id, property_nombre_id.get("favorite_music"), p.favorite_music);
				save_attribute(id, property_nombre_id.get("favorite_movies"), p.favorite_movies);
				save_attribute(id, property_nombre_id.get("favorite_tv_shows"), p.favorite_tv_shows);
				save_attribute(id, property_nombre_id.get("favorite_books"), p.favorite_books);
				save_attribute(id, property_nombre_id.get("favorite_quote"), p.favorite_quote);
				save_attribute(id, property_nombre_id.get("languages"), p.languages);
			}
			
			ps.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}	
	}
	private static void save_attribute(int id_node, int id_property, String value) {

		if(value != null && !value.equals("")){
			value = value.trim();
			
			try{
	
				PreparedStatement s = conn.prepareStatement(
						"INSERT INTO attribute (ID_node, ID_property, value) VALUES (?,?,?);");
				
				
				s.setInt(1, id_node);
				s.setInt(2, id_property);
				s.setString(3, value);

				try{
					s.executeUpdate(); 
				}catch(Exception e){
					e.printStackTrace();
				}
				s.close();
			} catch(Exception e){
				e.printStackTrace();
			}
		}
		
	}
	public static void save_edges_r(ArrayList<Relationship> relaciones) {
		for(Relationship r : relaciones){
			save_edge(r.profile_a, r.profile_b);
		
		}
	}
	private static void save_edge(String profile_a, String profile_b) {
		int id_a = 0, id_b = 0;
		try{
			id_a = get_ID(profile_a);
			id_b = get_ID(profile_b);

			PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO edge (ID_node_1, ID_property, ID_node_2) VALUES (?,?,?);");
			
			ps.setInt(1, id_a);
			ps.setInt(2, property_nombre_id.get("Relationship"));
			ps.setInt(3, id_b);
			
			ps.execute();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	private static void add_ID(String url, int ID){
		if (url_id.size() == 10000){
			url_id = new Hashtable<String, Integer>();
		}
		if(!url_id.contains(url))
			url_id.put(url, ID);
	}
	private static int get_ID(String url) {
		int ID = -1;

		if(url_id.contains(url)){
			return url_id.get(url);
		}
		else{
			try {
				PreparedStatement p1 = conn.prepareStatement("SELECT ID FROM node WHERE nombre = ?");
				p1.setString(1, url);
				ResultSet r1 = p1.executeQuery();
				if(r1.next()){
					ID = r1.getInt(1);
					add_ID(url, ID);
				}
				else{
					PreparedStatement p2 = conn.prepareStatement("INSERT INTO node (nombre) VALUES (?) RETURNING ID");
					p2.setString(1, url);
					ResultSet r2 = p2.executeQuery();
					if(r2.next()){
						ID = r2.getInt(1);
						add_ID(url, ID);
					}
					p2.close();
				}
				p1.close();
				
				return ID;
			} catch (Exception e) {
				e.printStackTrace();
				return ID;
			}
		}
	}
	public static int get_ID_max_out_degree() {
		int ID = 0;
		
		try {
			PreparedStatement p = conn.prepareStatement("SELECT id FROM node WHERE out_degree = (SELECT MAX(out_degree) from node)");
			ResultSet r = p.executeQuery();
			if(r.next()){
				ID = r.getInt(1);
			}
			p.close();
			
			return ID;
		} catch (Exception e) {
			e.printStackTrace();
			return ID;
		}
	}
	public static ArrayList<Edge> get_FRIENDSHIPS_of(int id) {
		ArrayList<Edge> edges = new ArrayList<Edge>();
		
		try {
			PreparedStatement p = conn.prepareStatement("SELECT * FROM edge WHERE id_node_1 = ? ORDER BY out_degree");
			p.setInt(1, id);
			
			ResultSet r = p.executeQuery();
			while(r.next()){
				edges.add(new Edge(r.getInt(1), r.getInt(2), r.getInt(3), r.getInt(4)));
			}
			p.close();
			
			return edges;
		} catch (Exception e) {
			e.printStackTrace();
			return edges;
		}
	}
	public static void save_edges(ArrayList<Edge> edges) {
		
	}
	public static int get_NODE_DB_size() {
		int n = 0;
		
		try{
			PreparedStatement p = conn.prepareStatement("SELECT COUNT(*) FROM node");
			ResultSet r = p.executeQuery();
			
			if(r.next()){
				n = r.getInt(1);
			}
			p.close();
			
			return n;
		}catch(Exception e){
			e.printStackTrace();
			return 0;
		}
	}
	public static ArrayList<Integer> get_FRIENDS_of(int id) {
//		try{
//			PreparedStatement p = conn.prepareStatement("SELECT COUNT(*) FROM node");
//			ResultSet r = p.executeQuery();
//			
//			if(r.next()){
//				n = r.getInt(1);
//			}
//			p.close();
//			
//			return n;
//		}catch(Exception e){
//			e.printStackTrace();
//			return 0;
//		}
		return null;
	}
	public static void calculate_degree(){		
		try {
			PreparedStatement p = conn.prepareStatement("SELECT id FROM node");
			ResultSet r = p.executeQuery();
			
			int i = 0;
			while(r.next()){
				int id = r.getInt(1);
				System.out.println(i++ + " " + id);
				calculate_degree_of(id);
			}
			p.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void calculate_degree_of(int id){
		try{
			PreparedStatement p = conn.prepareStatement("SELECT COUNT(*) AS degree " +
					"FROM (" +
					"SELECT id_node_1 AS id FROM edge WHERE id_node_2 = ?" +
					"UNION " +
					"SELECT id_node_2 AS id FROM edge WHERE id_node_1 = ?) temp");
			
			p.setInt(1, id);
			p.setInt(2, id);
			
			ResultSet r = p.executeQuery();

			if(r.next()){
				set_degree_of(id, r.getInt(1));
			}
			p.close();
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void set_degree_of(int id, int degree){
		try{
			PreparedStatement p = conn.prepareStatement("UPDATE node SET degree=? WHERE id=?");
			
			p.setInt(1, degree);
			p.setInt(2, id);
			
			p.execute();
			p.close();
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	public static ResultSet get_properties(){
		try {
			Statement s = conn.createStatement();
			ResultSet r = s.executeQuery("SELECT * FROM property");
			return r;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	public static ResultSet get_nodes(){
		try {
			Statement s = conn.createStatement();
			ResultSet r = s.executeQuery("SELECT * FROM node");
			return r;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	public static ResultSet get_attributes(){
		try {
			Statement s = conn.createStatement();
			ResultSet r = s.executeQuery("SELECT * FROM attribute");
			return r;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	public static ResultSet get_edges(int range, int block){
		try {
			Statement s = conn.createStatement();
			System.out.println("SELECT * FROM edge LIMIT " + block + " OFFSET " + range);
			ResultSet r = s.executeQuery("SELECT * FROM edge LIMIT " + block + " OFFSET " + range);
			return r;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
//		System.out.println("SELECT * FROM edge LIMIT " + range + "," + block);
//		return null;
	}
}