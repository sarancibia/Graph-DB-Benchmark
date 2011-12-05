package data_generator.database;
import java.io.FileNotFoundException;

import com.sparsity.dex.gdb.AttributeKind;
import com.sparsity.dex.gdb.Condition;
import com.sparsity.dex.gdb.DataType;
import com.sparsity.dex.gdb.Database;
import com.sparsity.dex.gdb.Dex;
import com.sparsity.dex.gdb.DexConfig;
import com.sparsity.dex.gdb.Graph;
import com.sparsity.dex.gdb.Objects;
import com.sparsity.dex.gdb.ObjectsIterator;
import com.sparsity.dex.gdb.Session;
import com.sparsity.dex.gdb.Value;

import data_generator.objects.Relationship;


public class DEX_DATABASE {
	static String LICENCE = "M1YYF-XBD45-JSZV9-5H934";
	
	static String DB_NAME = "Hi5_Graph.DEX";
	
	static String PERSON = "Persona";
	static String PERSON_ID = "ID";
	static String FRIENDSHIP = "amistad";
	
	static int OID_PERSON;
	static int OID_FRIENDSHIP;
	static int OID_PERSON_ID;
	
	static Database db = null;
	static Session s = null;
	static Graph g = null;

	public static void open(String db_name){
		if(db == null || db.isClosed()){
	        DexConfig conf = new DexConfig();
	        conf.setLicense(LICENCE);
	        Dex dex = new Dex(conf);
	        
	        try { 
				db = dex.open(DB_NAME, false);
				
			    s = db.newSession();
			    g = s.getGraph();

			    
				OID_PERSON = g.findType(PERSON);
				OID_PERSON_ID = g.findAttribute(OID_PERSON, PERSON_ID);
				OID_FRIENDSHIP = g.findType(FRIENDSHIP);
				
				//System.out.println("ABRE");

			} catch (Exception e) {
				try {
					db = dex.create(DB_NAME, DB_NAME);

			        s = db.newSession();
			        g = s.getGraph();
					
					OID_PERSON = g.newNodeType(PERSON);
					OID_PERSON_ID = g.newAttribute(OID_PERSON, PERSON_ID, DataType.String, AttributeKind.Unique);
					OID_FRIENDSHIP = g.newEdgeType(FRIENDSHIP, true, true);
					
					//System.out.println("CREA");

				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	}
	public static void close(){	
        db.close(); 
	}
	public static void save_friendship(Relationship a) {
		if(db != null && !db.isClosed()){
	        Value profile_a = new Value();
	        profile_a.setString(a.profile_a);
	        Value profile_b = new Value();
	        profile_b.setString(a.profile_b);
	        
	        long pa, pb;
	        
	        if(!exist(g, profile_a)){
		        pa = g.newNode(OID_PERSON);
		        g.setAttribute(pa, OID_PERSON_ID, profile_a);
	        }
	        else{
	        	pa = g.findObject(OID_PERSON, profile_a);
	        }
	
	        if(!exist(g, profile_b)){
		        pb = g.newNode(OID_PERSON);
		        g.setAttribute(pb, OID_PERSON_ID, profile_b);
	        }
	        else{
	        	pb = g.findObject(OID_PERSON, profile_b);
	        }
	
	        g.newEdge(OID_FRIENDSHIP, pa, pb);
	        
		}
		
	}
	private static boolean exist(Graph g, Value profile_a) {	
		Objects os = g.select(OID_PERSON_ID, Condition.Equal, profile_a);
		ObjectsIterator osi = os.iterator();
		
		return osi.hasNext();
	}



}
