package com.gnode.test;

import rpc.RpcIOCommons;

public class TraversalTest {

	public static void main(String[] args) {
		
		if (args.length != 1) {
			System.out.println("Param Error! Program will exit.");
			System.out.println("Usage: TraversalTest vertexID");
			return;
		}
		
		Utilities.traverseGraph_Test(args[0]);
		
		RpcIOCommons.stop();
	}

}
