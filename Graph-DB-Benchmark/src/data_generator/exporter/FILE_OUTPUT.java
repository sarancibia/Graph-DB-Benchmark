package data_generator.exporter;
import java.io.RandomAccessFile;
import java.util.Date;


public class FILE_OUTPUT {
	public static RandomAccessFile disco = null;
	
	@SuppressWarnings("deprecation")
	public static void escribir(String s){
		Date d = new Date();
		try {
			disco = new RandomAccessFile(d.getYear()+""+d.getMonth()+""+d.getDate(),"rw");
			disco.seek(disco.length());
			disco.writeBytes(s+"\n");
			
		} catch (Exception e) {
			
		}
		
	}
}
