package com.neo4j.tj.test;

import java.util.Date;

public class DataImportTest {

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Param Error! Program will exit.");
			System.out.println("Usage: DataImportTest masterIP filePath");
			return;
		}
		Long beforeDate = (new Date()).getTime();
		Utilities.createGraphFromFile(args[0],args[1]);
		System.out.println("************Elapsed Time(ms):"+((new Date()).getTime() - beforeDate)+"************");
	}

}
