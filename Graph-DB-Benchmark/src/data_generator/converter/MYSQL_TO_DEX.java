package data_generator.converter;


public class MYSQL_TO_DEX {
	public static void main(String[] args) {
		//int inicio = Integer.parseInt(args[0]);
		//int rango = Integer.parseInt(args[1]);
		
		long ini = System.currentTimeMillis();
		//int restan =  MYSQL_DATABASE.relaciones_to_DEX(inicio, rango);
		long ter = System.currentTimeMillis();
		
		System.out.println(ter-ini);
		//System.out.println(restan);
	}
}
