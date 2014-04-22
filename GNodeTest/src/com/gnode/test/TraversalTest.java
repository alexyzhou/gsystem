package com.gnode.test;

import rpc.RpcIOCommons;

public class TraversalTest {

	public static void main(String[] args) {
		
		if (args.length != 2) {
			System.out.println("Param Error! Program will exit.");
			System.out.println("Usage: TraversalTest vertexID level");
			return;
		}
		
		Utilities.traverseGraph_Test(args[0], args[1]);
		
		RpcIOCommons.stop();
	}

}
