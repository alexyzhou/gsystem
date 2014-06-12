package com.gnode.test;

import rpc.RpcIOCommons;

public class TraversalTest {

	public static void main(String[] args) {
		
		if (args.length != 4) {
			System.out.println("Param Error! Program will exit.");
			System.out.println("Usage: TraversalTest vertexID level target isDetailed");
			return;
		}
		
		Utilities.traverseGraph_Test(args[0], args[1], args[2], args[3]);
		
		RpcIOCommons.stop();
	}

}
