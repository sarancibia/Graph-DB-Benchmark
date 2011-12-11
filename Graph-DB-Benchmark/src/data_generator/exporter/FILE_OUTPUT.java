package data_generator.exporter;
import java.io.RandomAccessFile;
import java.util.Date;


public class FILE_OUTPUT {
	public static RandomAccessFile disk = null;
	
	@SuppressWarnings("deprecation")
	public static void write(String s){
		Date d = new Date();
		try {
			disk = new RandomAccessFile(d.getYear()+""+d.getMonth()+""+d.getDate(),"rw");
			disk.seek(disk.length());
			disk.writeBytes(s+"\n");
			
		} catch (Exception e) {
			
		}
		
	}
}
