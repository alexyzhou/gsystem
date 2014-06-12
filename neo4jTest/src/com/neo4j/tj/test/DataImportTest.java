package com.neo4j.tj.test;

import java.util.Date;

public class DataImportTest {

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Param Error! Program will exit.");
			System.out.println("Usage: DataImportTest masterIP filePath");
			return;
		}
		String masterIP = "127.0.0.1";
		String filePath = "/Users/alex/Documents/exData/500000.csv";
		Long beforeDate = (new Date()).getTime();
		//Utilities.createGraphFromFile(args[0],args[1]);
		Utilities.createGraphFromFile(masterIP,filePath);
		System.out.println("************Elapsed Time(ms):"+((new Date()).getTime() - beforeDate)+"************");
	}

}
