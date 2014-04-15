package com.neo4j.tj.test;

import java.net.URISyntaxException;
import java.util.Date;

import com.neo4j.tj.other.TraversalDefinition;

public class TraversalTest {

	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("Param Error! Program will exit.");
			System.out.println("Usage: TraversalTest vertexID Order[D;B] maxDepth");
			return;
		}
		String order = TraversalDefinition.BREADTH_FIRST;
		if (args[1].equals("D")) {
			order = TraversalDefinition.DEPTH_FIRST;
		}
		int maxDepth = Integer.parseInt(args[2]);
		Long beforeDate = (new Date()).getTime();
		try {
			Utilities.traverseGraph(args[0], order, maxDepth);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("************Elapsed Time(ms):"+((new Date()).getTime() - beforeDate)+"************");
		
	}

}
