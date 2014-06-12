package com.gnode.test.rpc;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import rpc.GClientProtocol;
import system.SystemConf;
import utilities.Net_Utilities;

public class GClientRPC implements GClientProtocol{
	
	protected Server rpcServer;
	protected Boolean requestFinished;
	protected String isDetailed;
	//protected Thread rpcThread;
	
	public long traverseBeginTime;
	public String target;
	
	protected static GClientRPC _instance = null;
	public static GClientRPC getInstance(long beginTime, String target, String isDetailed) {
		if (_instance == null) {
			_instance = new GClientRPC(beginTime,target, isDetailed);
		} else {
			_instance.traverseBeginTime = beginTime;
		}
		return _instance;
	}
	
	public GClientRPC(long beginTime, String target, String isDetailed) {
		this.traverseBeginTime = beginTime;
		this.target = target;
		this.isDetailed = isDetailed;
		requestFinished = false;
		try {
			rpcServer = RPC.getServer(GClientRPC.this,
					Net_Utilities.obtainLocalIP(),
					SystemConf.getInstance().RPC_GCLIENT_PORT,
					new Configuration());
			rpcServer.start();
			//rpcServer.join();
		} catch (IOException e) {
			e.printStackTrace();
		}
		/*rpcThread = new Thread(new Runnable() {

			@Override
			public void run() {

				try {
					rpcServer = RPC.getServer(GClientRPC.this,
							Net_Utilities.obtainLocalIP(),
							SystemConf.getInstance().RPC_GCLIENT_PORT,
							new Configuration());
					rpcServer.start();
					rpcServer.join();
				} catch (IOException e) {

					e.printStackTrace();
				} catch (InterruptedException e) {

					e.printStackTrace();
				}

			}
		});
		rpcThread.start();*/
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		return SystemConf.RPC_VERSION;
	}

	@Override
	public void traverseDidFinished(String result) {
		Long elapsedTime = (new Date()).getTime() - traverseBeginTime;
		Process p;
		try {
			p = Runtime.getRuntime().exec("/home/hadoop/GNode/monitor/printTraverse.sh "+target+" "+elapsedTime + " " + isDetailed);
			p.waitFor();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
					while (rpcServer.getNumOpenConnections() > 0) {
						try {
							Thread.sleep(500);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					//We finished!
					System.out.println("Execute Stop!");
					rpcServer.stop();
			}
		}).start();
	}
}
