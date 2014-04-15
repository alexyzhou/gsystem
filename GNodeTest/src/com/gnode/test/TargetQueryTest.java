package com.gnode.test;

import java.util.Date;

import rpc.RpcIOCommons;

public class TargetQueryTest {

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Param Error! Program will exit.");
			System.out.println("Usage: TargetQueryTest vertexID");
			return;
		}
		Long beforeDate = (new Date()).getTime();
		Utilities.queryGraph_Test(args[0]);
		System.out.println("************Elapsed Time(ms):"+((new Date()).getTime() - beforeDate)+"************");
		RpcIOCommons.stop();
	}

}
