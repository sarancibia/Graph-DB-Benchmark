package data_generator.converter;

import data_generator.database.MYSQL_DATABASE;

public class MYSQL_TO_DEX {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int inicio = Integer.parseInt(args[0]);
		int rango = Integer.parseInt(args[1]);
		
		long ini = System.currentTimeMillis();
		int restan =  MYSQL_DATABASE.relaciones_to_DEX(inicio, rango);
		long ter = System.currentTimeMillis();
		
		System.out.println(ter-ini);
		System.out.println(restan);

//		int inicio = 0;
//		int rango = 500;
//		System.out.println(DB.relaciones_to_DEX(inicio, rango));
		
		//DB.emulaGrafo();
		
		
		//DEX.prueba();
	}
}
