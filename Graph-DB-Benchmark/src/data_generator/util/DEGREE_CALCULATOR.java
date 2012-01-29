package data_generator.util;

import data_generator.database.POSTGRESQL_DATABASE_HI5;

public class DEGREE_CALCULATOR {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		calculate();
	}

	private static void calculate() {
		POSTGRESQL_DATABASE_HI5.connect();
		POSTGRESQL_DATABASE_HI5.calculate_degree();
		POSTGRESQL_DATABASE_HI5.disconnect();
	}
}