package utilities;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import data.io.TraverseJobIntermediateResult;
import rpc.GClientProtocol;
import rpc.GServerProtocol;
import system.GClient;
import system.SystemConf;
import test.Debug;

public class Log_Utilities {
	
	public static String LOG_HEADER_DEBUG = "DEBUG";
	public static String LOG_HEADER_INFO = "INFO";

	public static void printGServerLog(String header, String log) {
		if (Debug.printDetailedLog)
		System.out.println("[" + SystemConf.getTime() + "][gSERVER][" + header + "] " + log);
	}
	
	public static void printGServerTraversalLog(TraverseJobIntermediateResult tempResult) {
		// we reached the root layer
		System.out.println("[" + SystemConf.getTime()
				+ "][gSERVER][Graph Traversal] Finished!");
		System.out.println("[" + SystemConf.getTime()
				+ "][gSERVER][Graph Traversal] Job ID: "
				+ tempResult.param.jobID.toString());
		Collections.sort(tempResult.result);
		System.out.println("[" + SystemConf.getTime()
				+ "][gSERVER][Graph Traversal] Result: "
				+ tempResult.genResultString());
//		System.out
//				.println("["
//						+ SystemConf.getTime()
//						+ "][gSERVER][Graph Traversal] [********TimeCost(ms)********]: "
//						+ ((new Date().getTime()) - tempResult.param.beginTime));
		
		//notify the client
		
		System.out
		.println("["
				+ SystemConf.getTime()
				+ "][gSERVER][Graph Traversal] [Traverse NotifyFinish]: ");
		
		InetSocketAddress address = new InetSocketAddress(tempResult.param.client_ip,
				SystemConf.getInstance().RPC_GCLIENT_PORT);
		GClientProtocol proxy;
		try {
			proxy = (GClientProtocol) RPC.waitForProxy(
					GClientProtocol.class, SystemConf.RPC_VERSION, address,
					new Configuration());
			proxy.traverseDidFinished(tempResult.genResultString());
			RPC.stopProxy(proxy);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void printGMasterLog(String header, String log) {
		if (Debug.printDetailedLog)
		System.out.println("[" + SystemConf.getTime() + "][MASTER][" + header + "] " + log);
	}
	
	public static void printGMasterRuntimeLog(String log) {
		System.out.println("[" + SystemConf.getTime() + "][MASTER] "+log);
	}
	
	public static void printGServerRuntimeLog(String log) {
		System.out.println("[" + SystemConf.getTime() + "][gServer] "+log);
	}

}
