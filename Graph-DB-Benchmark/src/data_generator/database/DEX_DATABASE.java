package data_generator.database;
import java.io.FileNotFoundException;
import java.util.ArrayList;

import com.sparsity.dex.gdb.AttributeKind;
import com.sparsity.dex.gdb.Condition;
import com.sparsity.dex.gdb.DataType;
import com.sparsity.dex.gdb.Database;
import com.sparsity.dex.gdb.DefaultExport;
import com.sparsity.dex.gdb.Dex;
import com.sparsity.dex.gdb.DexConfig;
import com.sparsity.dex.gdb.Graph;
import com.sparsity.dex.gdb.Objects;
import com.sparsity.dex.gdb.ObjectsIterator;
import com.sparsity.dex.gdb.Session;
import com.sparsity.dex.gdb.Value;

import data_generator.objects.Amistad;


public class DEX_DATABASE {
	static String LICENCE = "M1YYF-XBD45-JSZV9-5H934";
	
	static String DB_NAME = "Hi5_Graph.DEX";
	
	static String PERSONA = "Persona";
	static String PERSONA_ID = "ID";
	static String AMISTAD = "amistad";
	
	static int OID_PERSONA;
	static int OID_AMISTAD;
	static int OID_PERSONA_ID;
	
	static Database db = null;
	static Session s = null;
	static Graph g = null;
	
	
	public static void prueba(){
        DexConfig conf = new DexConfig();
        conf.setLicense(LICENCE);

		try {
			Dex dex = new Dex(conf);
			db = dex.create(DB_NAME, DB_NAME);
		    s = db.newSession();
		    g = s.getGraph();
		   
			OID_PERSONA = g.newNodeType(PERSONA);
			OID_PERSONA_ID = g.newAttribute(OID_PERSONA, PERSONA_ID, DataType.String, AttributeKind.Basic);
			OID_AMISTAD = g.newEdgeType(AMISTAD, true, true);
			
			s.commit();
		    db.close();
		    dex.close();    
		    
		    Value aa = new Value(), bb = new Value();
			for(int i = 0 ; i < 500000; i++){	
				dex = new Dex(conf);
				db = dex.open(DB_NAME, false);

			    
				for(int j = 0; j < 30; j++){
				    s = db.newSession();;
				    g = s.getGraph();

				    
					long a = g.newNode(OID_PERSONA);
					long b = g.newNode(OID_PERSONA);
				
					//aa = new Value();
//					aa.setString(Math.random() + "");
//					bb.setString(Math.random() + "");

//					aa = new Value();
//					bb = new Value();
					
					aa.setStringVoid(""+j);
					bb.setStringVoid(""+j);

					//System.out.println(aa);
					//aa.delete();
					
					
					
//					Value bb = new Value();
//					bb.setString("bb");
					g.setAttribute(a, OID_PERSONA_ID, aa);
					g.setAttribute(b, OID_PERSONA_ID, bb);
				    
					g.newEdge(OID_AMISTAD, a, b);

					s.commit();
					s.close();
				
				}

				db.close();
				dex.close();
				
				if(i%1000 == 0)
				System.out.println(i);
				//System.out.println("Guardo " + i);
			}
			//estadistica();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public static void MySQLToDEX() {
        DexConfig conf = new DexConfig();
        conf.setLicense(LICENCE);

		try {
			Dex dex = new Dex(conf);
			db = dex.create(DB_NAME, DB_NAME);
		    s = db.newSession();
		    g = s.getGraph();
		   
			OID_PERSONA = g.newNodeType(PERSONA);
			OID_PERSONA_ID = g.newAttribute(OID_PERSONA, PERSONA_ID, DataType.String, AttributeKind.Basic);
			OID_AMISTAD = g.newEdgeType(AMISTAD, true, true);
			
			s.commit();
		    db.close();
		    dex.close();    
		    
		    //Value aa = new Value(), bb = new Value();
		    
		    int i = 0; 
		    ArrayList<Amistad> relaciones = MYSQL_DATABASE.getRelaciones(i++, 30);
		    while(relaciones.size() > 0){
		    	dex = new Dex(conf);
				db = dex.open(DB_NAME, false);
			    s = db.newSession();
			    
				for(int j = 0; j < relaciones.size(); j++){
				    g = s.getGraph();

				    
					long a = g.newNode(OID_PERSONA);
					long b = g.newNode(OID_PERSONA);
				    
					g.newEdge(OID_AMISTAD, a, b);


					db.close();
					dex.close();
				}
				relaciones = new ArrayList<Amistad>();
		    	relaciones = MYSQL_DATABASE.getRelaciones(i++, 30);
		    }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public static void open(String db_name){
		if(db == null || db.isClosed()){
	        DexConfig conf = new DexConfig();
	        conf.setLicense(LICENCE);
	        Dex dex = new Dex(conf);
	        
	        try { 
				db = dex.open(DB_NAME, false);
				
			    s = db.newSession();
			    g = s.getGraph();

			    
				OID_PERSONA = g.findType(PERSONA);
				OID_PERSONA_ID = g.findAttribute(OID_PERSONA, PERSONA_ID);
				OID_AMISTAD = g.findType(AMISTAD);
				
				//System.out.println("ABRE");

			} catch (Exception e) {
				try {
					db = dex.create(DB_NAME, DB_NAME);

			        s = db.newSession();
			        g = s.getGraph();
					
					OID_PERSONA = g.newNodeType(PERSONA);
					OID_PERSONA_ID = g.newAttribute(OID_PERSONA, PERSONA_ID, DataType.String, AttributeKind.Unique);
					OID_AMISTAD = g.newEdgeType(AMISTAD, true, true);
					
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
	public static void amistad_guardar(Amistad a) {
		if(db != null && !db.isClosed()){
	        Value profile_a = new Value();
	        profile_a.setString(a.profile_a);
	        Value profile_b = new Value();
	        profile_b.setString(a.profile_b);
	        
	        long pa, pb;
	        
	        if(!existe(g, profile_a)){
		        pa = g.newNode(OID_PERSONA);
		        g.setAttribute(pa, OID_PERSONA_ID, profile_a);
	        }
	        else{
	        	pa = g.findObject(OID_PERSONA, profile_a);
	        }
	
	        if(!existe(g, profile_b)){
		        pb = g.newNode(OID_PERSONA);
		        g.setAttribute(pb, OID_PERSONA_ID, profile_b);
	        }
	        else{
	        	pb = g.findObject(OID_PERSONA, profile_b);
	        }
	
	        g.newEdge(OID_AMISTAD, pa, pb);
	        
		}
		
	}
	private static boolean existe(Graph g, Value profile_a) {	
		Objects os = g.select(OID_PERSONA_ID, Condition.Equal, profile_a);
		ObjectsIterator osi = os.iterator();
		
		return osi.hasNext();
	}
	public static void estadistica() {
		open(DB_NAME);
        
		System.out.println("Número de relaciones de amistad: " + g.countEdges());
		System.out.println("Número de personas: " + g.countNodes());
		
		DefaultExport de = new DefaultExport();
		de.release();
		//g.export("Hi5.graphml", ExportType.GraphML, de);
		
		close();
		
	}


}
