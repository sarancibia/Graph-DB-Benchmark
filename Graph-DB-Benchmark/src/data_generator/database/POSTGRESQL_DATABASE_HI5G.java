package data_generator.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import data_generator.objects.Edge;
import data_generator.objects.Node;

public class POSTGRESQL_DATABASE_HI5G {
    public static String driver = "org.postgresql.Driver";
    public static String connectString = "jdbc:postgresql://localhost:5432/hi5g";
    public static String user = "sarancibia";
    public static String password = "sa2335";
    public static Connection conn;
    public static Statement stmt;
    public static Boolean connected = false;
    
	
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
	public static void create_unidirectional_db(){
    	try{
    		Statement s = conn.createStatement();
    		s.execute("CREATE TABLE node" +
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
    				"" +
    				"CREATE INDEX hash_node" +
    				"  ON node" +
    				"  USING hash" +
    				"  (nombre);" +
    				" " +
    				"CREATE TABLE property" +
    				"(" +
    				"  id serial NOT NULL," +
    				"  nombre character varying(500) NOT NULL," +
    				"  counter integer NOT NULL DEFAULT 0," +
    				"  CONSTRAINT property_pkey PRIMARY KEY (id)," +
    				"  CONSTRAINT property_nombre_key UNIQUE (nombre)" +
    				")" +
    				"WITH (" +
    				"  OIDS=FALSE" +
    				");" +
    				"ALTER TABLE property OWNER TO sarancibia;" +
    				"" +
    				"CREATE TABLE attribute" +
    				"(" +
    				"  id_node integer NOT NULL," +
    				"  id_property integer NOT NULL," +
    				"  \"value\" text NOT NULL," +
    				"  CONSTRAINT attribute_pkey PRIMARY KEY (id_node, id_property)" +
    				")" +
    				"WITH (" +
    				"  OIDS=FALSE" +
    				");" +
    				"ALTER TABLE attribute OWNER TO sarancibia;" +
    				"" +
    				"CREATE TABLE edge" +
    				"(" +
    				"  id serial NOT NULL," +
    				"  id_node_1 integer NOT NULL," +
    				"  id_property integer NOT NULL," +
    				"  id_node_2 integer NOT NULL," +
    				"  CONSTRAINT edge_pkey PRIMARY KEY (id)," +
    				"  CONSTRAINT edge_relationship UNIQUE (id_node_1, id_node_2)" +
    				")WITH (" +
    				"  OIDS=FALSE" +
    				");" +
    				"ALTER TABLE edge OWNER TO sarancibia;" +
    				"" +
    				"CREATE OR REPLACE RULE insert_new_edge AS" +
    				" 	ON INSERT TO edge DO (" +
    				" 		UPDATE node SET out_degree = node.out_degree + 1 WHERE node.id = new.id_node_1;" +
    				"		UPDATE node SET in_degree = node.in_degree + 1 WHERE node.id = new.id_node_2;" +
    				"		UPDATE property SET counter = counter+1 WHERE ID=new.id_property;" +
    				"	);");
    		}catch(Exception e){
    		System.out.println(e.getMessage());
    		e.printStackTrace();
    	}
    }
	public static void create_bidirectional_db(){
    	try{
    		Statement s = conn.createStatement();
    		s.execute("CREATE TABLE node" +
    				"(" +
    				"  id serial NOT NULL," +
    				"  nombre character varying(500) NOT NULL," +
    				"  nro_attributes integer NOT NULL DEFAULT 0," +
    				"  degree integer NOT NULL DEFAULT 0," +
    				"  CONSTRAINT node_pkey PRIMARY KEY (id)," +
    				"  CONSTRAINT node_nombre_key UNIQUE (nombre)" +
    				")" +
    				"WITH (" +
    				"  OIDS=FALSE" +
    				");" +
    				"ALTER TABLE node OWNER TO sarancibia;" +
    				"" +
    				"CREATE INDEX hash_node" +
    				"  ON node" +
    				"  USING hash" +
    				"  (nombre);" +
    				"" +
    				"" +
    				"CREATE TABLE property" +
    				"(" +
    				"  id serial NOT NULL," +
    				"  nombre character varying(500) NOT NULL," +
    				"  counter integer NOT NULL DEFAULT 0," +
    				"  CONSTRAINT property_pkey PRIMARY KEY (id)," +
    				"  CONSTRAINT property_nombre_key UNIQUE (nombre)" +
    				")" +
    				"WITH (" +
    				"  OIDS=FALSE" +
    				");" +
    				"ALTER TABLE property OWNER TO sarancibia;" +
    				"" +
    				"CREATE TABLE attribute" +
    				"(" +
    				"  id_node integer NOT NULL," +
    				"  id_property integer NOT NULL," +
    				"  \"value\" text NOT NULL," +
    				"  CONSTRAINT attribute_pkey PRIMARY KEY (id_node, id_property)" +
    				")" +
    				"WITH (" +
    				"  OIDS=FALSE" +
    				");" +
    				"ALTER TABLE attribute OWNER TO sarancibia;" +
    				"" +
    				"CREATE TABLE edge" +
    				"(" +
    				"  id serial NOT NULL," +
    				"  id_node_1 integer NOT NULL," +
    				"  id_property integer NOT NULL," +
    				"  id_node_2 integer NOT NULL," +
    				"  CONSTRAINT edge_pkey PRIMARY KEY (id)" +
    				")WITH (" +
    				"  OIDS=FALSE" +
    				");" +
    				"ALTER TABLE edge OWNER TO sarancibia;" +
    				"" +
    				"CREATE OR REPLACE RULE insert_new_edge AS " +
    				" 	ON INSERT TO edge DO (" +
    				" 		UPDATE node SET degree = node.degree + 1 WHERE node.id = new.id_node_1;" +
    				"		UPDATE node SET degree = node.degree + 1 WHERE node.id = new.id_node_2;" +
    				"		UPDATE property SET counter = counter+1 WHERE ID=new.id_property;" +
    				"		DELETE FROM edge WHERE id_node_1 = new.id_node_2 AND id_node_2 = new.id_node_1;" +
    				"    		);" +
    				"" +
    				"CREATE OR REPLACE RULE delete_edge AS " +
    				"ON DELETE TO edge DO (" +
    				" 		UPDATE node SET degree = node.degree - 1 WHERE node.id = old.id_node_1;" +
    				"		UPDATE node SET degree = node.degree - 1 WHERE node.id = old.id_node_2;" +
    				"		UPDATE property SET counter = counter-1 WHERE ID=old.id_property;" +
    				"	);" +
    				"" +
    				"CREATE OR REPLACE RULE insert_new_attribute AS " +
    				"	ON INSERT TO attribute DO (" +
    				"		UPDATE property SET counter = counter+1 WHERE ID=new.id_property;" +
    				"		UPDATE node SET nro_attributes = nro_attributes+1 WHERE ID=new.id_node;" +
    				"	);");
    		}catch(Exception e){
    		System.out.println(e.getMessage());
    		e.printStackTrace();
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
			e.getMessage();
			e.printStackTrace();
			return ID;
		}
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
	public static boolean is_connected(){
   		if(connected) 
   			return true;
    	else 
    		return false;
    }
	private static void save_property(int id, String nombre, int counter) {
		try{

			PreparedStatement s = conn.prepareStatement(
					"INSERT INTO property (id, nombre, counter) VALUES (?,?,?);");
			
			s.setInt(1, id);
			s.setString(2, nombre);
			s.setInt(3, counter);

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
	public static void save_properties() {
		try {
			ResultSet r = POSTGRESQL_DATABASE_HI5.get_properties();
			while(r.next()){
				save_property(r.getInt("id"), r.getString("nombre"), r.getInt("counter"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	private static void save_node(int id, String nombre, int nro_attributes) {
		try{

			PreparedStatement s = conn.prepareStatement(
					"INSERT INTO node (id, nombre, nro_attributes) VALUES (?,?,?);");
			
			s.setInt(1, id);
			s.setString(2, nombre);
			s.setInt(3, nro_attributes);

			try{
				s.executeUpdate(); 
			}catch(Exception e){
				System.out.println(id + " " + nombre + " " + nro_attributes);
				e.printStackTrace();
			}
			s.close();
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void save_nodes() {
		try {
			ResultSet r = POSTGRESQL_DATABASE_HI5.get_nodes();
			while(r.next()){
				save_node(r.getInt("id"), r.getString("nombre"), r.getInt("nro_attributes"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
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
	public static void save_attributes() {
		try {
			ResultSet r = POSTGRESQL_DATABASE_HI5.get_attributes();
			while(r.next()){
				save_attribute(r.getInt("id_node"),	r.getInt("id_property"), r.getString("value"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	private static void save_edge(int ID_node_1, int ID_property, int ID_node_2) {
		try{
			PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO edge (ID_node_1, ID_property, ID_node_2) VALUES (?,?,?);");
			
			ps.setInt(1, ID_node_1);
			ps.setInt(2, ID_property);
			ps.setInt(3, ID_node_2);
			
			ps.execute();
		}
		catch(Exception e){
			//e.printStackTrace();
		}
	}
	public static void save_unidirectional_edges(int range, int block) {
		try {
			ResultSet r = POSTGRESQL_DATABASE_HI5.get_edges(range, block);
			while(r != null && r.next()){
				try{
					save_edge(r.getInt("id_node_1"), r.getInt("id_property"), r.getInt("id_node_2"));
				}catch(Exception e){}
				try{
					save_edge(r.getInt("id_node_2"), r.getInt("id_property"), r.getInt("id_node_1"));
				}catch(Exception e){}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	public static void save_bidirectional_edges(int range, int block) {
		try {
			ResultSet r = POSTGRESQL_DATABASE_HI5.get_edges(range, block);
			while(r != null && r.next()){
				save_edge(r.getInt("id_node_1"), r.getInt("id_property"), r.getInt("id_node_2"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	public static ArrayList<Node> get_FRIENDS(int id){
		ArrayList<Node> friends = new ArrayList<Node>();
		
		try {
			PreparedStatement p = conn.prepareStatement("" +
					"SELECT edge.id_node_2, node.out_degree " +
					"FROM edge INNER JOIN node ON edge.id_node_2 = node.id " +
					"WHERE edge.id_node_1 = ?");
			
			p.setInt(1, id);
			
			ResultSet r = p.executeQuery();
			while(r.next()){
				friends.add(new Node(r.getInt("id_node_2"), "", 0, r.getInt("out_degree"), r.getInt("out_degree")));
			}
			p.close();
			
			return friends;
		} catch (Exception e) {
			e.getMessage();
			e.printStackTrace();
			return friends;
		}
	}
	public static float get_AVERANGE_DEGREE() {
		float averange=0;
		
		try {
			PreparedStatement p = conn.prepareStatement("SELECT AVG(out_degree) AS averange FROM node");
			ResultSet r = p.executeQuery();
			if(r.next()){
				averange = r.getFloat("averange");
			}
			p.close();
			
			return averange;
		} catch (Exception e) {
			e.getMessage();
			e.printStackTrace();
			return averange;
		}
	}
	public static ArrayList<Edge> get_PRUNING(int n) {
		ArrayList<Edge> edges = new ArrayList<Edge>();
		
		try {
			PreparedStatement p = conn.prepareStatement("" +
					"SELECT *  " +
					"FROM edge " +
					"WHERE id_node_1 IN (SELECT id FROM node ORDER BY out_degree DESC LIMIT ? OFFSET 0) AND " +
					"id_node_2 IN (SELECT id FROM node ORDER BY out_degree DESC LIMIT ? OFFSET 0)");
			
			p.setInt(1, n);
			p.setInt(2, n);
			
			ResultSet r = p.executeQuery();
			while(r.next()){
				edges.add(new Edge(r.getInt(1), r.getInt(2), r.getInt(3), r.getInt(4)));
			}
			p.close();
			
			return edges;
		} catch (Exception e) {
			e.getMessage();
			e.printStackTrace();
			return edges;
		}
		
	}
	public static ArrayList<Edge> get_FRIENDSHIPS_of(int id) {
		ArrayList<Edge> edges = new ArrayList<Edge>();
		
		try {
			PreparedStatement p = conn.prepareStatement("SELECT * FROM edge WHERE id_node_1 = ?");
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
	public static int get_degree_of(int id) {
		int degree = 0;
		try {
			PreparedStatement p = conn.prepareStatement("SELECT out_degree FROM node WHERE id = ?");
			p.setInt(1, id);
			
			ResultSet r = p.executeQuery();
			if(r.next()){
				degree = r.getInt(1);
			}
			p.close();
			
			return degree;
		} catch (Exception e) {
			e.printStackTrace();
			return degree;
		}
	}
	public static ArrayList<Edge> getDirectedEdges() {
		ArrayList<Edge> edges = new ArrayList<Edge>();
		
		try {
			PreparedStatement p = conn.prepareStatement("SELECT * FROM edge");
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
	public static ArrayList<Edge> getUndirectedEdges() {
		ArrayList<Edge> edges = new ArrayList<Edge>();
		
		try {
			PreparedStatement p = conn.prepareStatement("SELECT * FROM edge WHERE id_node_1 > id_node_2");
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
}