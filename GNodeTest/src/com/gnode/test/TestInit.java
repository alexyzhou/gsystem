package com.gnode.test;

import rpc.RpcIOCommons;

public class TestInit {

	public static void main(String[] args) {
		
		if (args.length != 2) {
			System.out.println("Param Error! Program will exit.");
			System.out.println("Usage: TestEnvInit dataSetID dataSetPath");
			return;
		}
		
		Utilities.insertDataSet_Test(args[0], args[1]);
		
		Utilities.insertDataSchema_Test();		
		Utilities.insertGraphSchema_Test();
		
		//queryGraph_Test();
		//traverseGraph_Test();
		
		RpcIOCommons.stop();
	}

}
