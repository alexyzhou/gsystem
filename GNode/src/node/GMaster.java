package node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import rpc.GMasterProtocol;
import rpc.GServerProtocol;
import rpc.RpcIOCommons;
import system.SystemConf;
import system.error.ErrorCode;
import test.Debug;
import utilities.Log_Utilities;
import zk.Lock;
import zk.LockFactory;
import zk.ZkObtainer;
import data.io.EdgeInfo;
import data.io.VertexInfo;
import data.writable.BPlusTreeStrStrWritable;
import data.writable.EdgeCollectionWritable;
import data.writable.StringMapWritable;
import data.writable.StringPairWritable;
import data.writable.VertexCollectionWritable;
import ds.bplusTree.BPlusTree;

public class GMaster extends GNode implements Runnable, GMasterProtocol {

	private class GServerInfo {
		public String ip;
		public boolean alive;
		public boolean isIndexServer;

		public GServerInfo(String ip) {
			this.ip = ip;
			this.alive = true;
			this.isIndexServer = false;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((ip == null) ? 0 : ip.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			GServerInfo other = (GServerInfo) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (ip == null) {
				if (other.ip != null)
					return false;
			} else if (!ip.equals(other.ip))
				return false;
			return true;
		}

		private GMaster getOuterType() {
			return GMaster.this;
		}
		
		

	}

	protected HashMap<GServerInfo, ArrayList<GServerInfo>> gServerList;
	protected BPlusTree<String, String> vGlobalTree;
	protected HashMap<String, Integer> gServerStorageMap;
	//protected BPlusTree<String, String> eGlobalTree;
	protected BPlusTree<String, String> dsPathIndex;

	protected StringMapWritable getVList() {
		StringMapWritable smw = new StringMapWritable();
		Set<String> keySet = vGlobalTree.getKeySet();
		for (String key : keySet) {
			smw.data.add(new StringPairWritable(key, vGlobalTree.get(key)));
		}
		return smw;
	}

//	protected StringMapWritable getEList() {
//		StringMapWritable smw = new StringMapWritable();
//		Set<String> keySet = eGlobalTree.getKeySet();
//		for (String key : keySet) {
//			smw.data.add(new StringPairWritable(key, eGlobalTree.get(key)));
//		}
//		return smw;
//	}

	protected Watcher zooWatcher = new Watcher() {

		@Override
		public void process(WatchedEvent event) {

			if (event.getType() == EventType.NodeChildrenChanged) {
				// Serverlist changed!
				Log_Utilities.printGMasterLog("MASTER", "[MASTER] NodeChildrenChanged!");
				scanServerList(event.getPath());
			}
		}
	};

	protected void scanServerList(String path) {
		try {
			List<String> children = zooKeeper.getChildren(path, true);
			ArrayList<GServerInfo> cloneList = new ArrayList<GServerInfo>();
			Set<GServerInfo> keySet = gServerList.keySet();
			for (GServerInfo key : keySet) {
				cloneList.add(key);
				for (GServerInfo info : gServerList.get(key)) {
					cloneList.add(info);
				}
			}
			for (GServerInfo info : cloneList) {
				info.alive = false;
			}
			for (String eve : children) {
				if (!eve.equals(LockFactory.MASTER_ID)) {
					String wl;
					try {
						wl = new String(zooKeeper.getData(path + "/" + eve,
								false, null));
						String[] ipv = wl.split(":");
						// GServer's IP is ipv[0]
						if (ipv[1].equals("lock")) {
							boolean isNewServer = true;

							for (GServerInfo info : cloneList) {
								if (ipv[0].equals(info.ip)) {
									info.alive = true;
									isNewServer = false;
									break;
								}
							}

							if (isNewServer == true) {
								// We've found a new Server
								addANewServer(ipv[0]);
							}
						}

					} catch (KeeperException e) {

						e.printStackTrace();
					} catch (InterruptedException e) {

						e.printStackTrace();
					}
				}
			}

			for (GServerInfo info : cloneList) {
				if (info.alive == false) {
					// we will delete dead server
					removeDeadServer(info);
				}
			}
		} catch (KeeperException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
	}

	protected void addANewServer(String ip) {
		gServerStorageMap.put(ip, 0);
		// TO DO
		Log_Utilities.printGMasterLog("MASTER", "AddaNewServer Called!");
		synchronized (gServerList) {
			Set<GServerInfo> keys = gServerList.keySet();
			for (GServerInfo info : keys) {
				if (SystemConf.inSameSubNet(info.ip, ip)) {
					gServerList.get(info).add(new GServerInfo(ip));
					GServerProtocol proxy;
					try {
						proxy = RpcIOCommons.getGServerProtocol(ip);
						proxy.announceIndexServer(info.ip);
						if (Debug.masterStopProxy)
							RPC.stopProxy(proxy);
						RpcIOCommons.freeGServerProtocol(ip);
						return;
					} catch (IOException e) {

						e.printStackTrace();
					}

				}
			}
			gServerList.put(new GServerInfo(ip), new ArrayList<GServerInfo>());
			GServerProtocol proxy;
			try {
				proxy = RpcIOCommons.getGServerProtocol(ip);
				proxy.assignIndexServer(
						new BPlusTreeStrStrWritable(vGlobalTree),
						new BPlusTreeStrStrWritable(dsPathIndex));
				if (Debug.masterStopProxy)
					RPC.stopProxy(proxy);
				RpcIOCommons.freeGServerProtocol(ip);
				return;
			} catch (IOException e) {

				e.printStackTrace();
			}
		}
	}

	protected void removeDeadServer(GServerInfo ip) {
		// TO DO

			Log_Utilities.printGMasterLog(Log_Utilities.LOG_HEADER_DEBUG, "removeDeadServer, ip="+ip.ip);
		
		gServerStorageMap.remove(ip);
		synchronized (gServerList) {
			Set<GServerInfo> keys = gServerList.keySet();
			for (GServerInfo info : keys) {
				if (info.equals(ip)) {
					// removing an Index Server, need to re-assign a new one
					double minUsage = Double.MAX_VALUE;
					GServerInfo targetIP = null;
					for (GServerInfo server : gServerList.get(info)) {
						try {
							GServerProtocol proxy = RpcIOCommons
									.getGServerProtocol(server.ip);
							double usage = proxy.reportUsageMark();
							if (usage < minUsage) {
								minUsage = usage;
								targetIP = server;
							}
							if (Debug.masterStopProxy)
								RPC.stopProxy(proxy);
							RpcIOCommons.freeGServerProtocol(server.ip);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					if (targetIP == null) {
						targetIP = gServerList.get(info).get(0);
						if (targetIP == null)
							return;
					}
					for (GServerInfo server : gServerList.get(info)) {
						try {
							GServerProtocol proxy = RpcIOCommons
									.getGServerProtocol(server.ip);
							if (server.ip.equals(targetIP.ip)) {
								proxy.assignIndexServer(
										new BPlusTreeStrStrWritable(vGlobalTree),
										new BPlusTreeStrStrWritable(dsPathIndex));
							} else {
								proxy.announceIndexServer(targetIP.ip);
							}
							RpcIOCommons.freeGServerProtocol(server.ip);
						} catch (IOException e) {

							e.printStackTrace();
						}
					}
					ArrayList<GServerInfo> clone = gServerList.get(info);
					clone.remove(targetIP);
					gServerList.remove(info);
					gServerList.put(targetIP, clone);
				} else if (SystemConf.inSameSubNet(info.ip, ip.ip)) {
					// removing a normal Server
					gServerList.get(info).remove(ip);
				}
			}
		}
	}

	public GMaster(Lock lock, String ip) throws Exception {
		super(lock, ip);
		// Init();
	}

	protected void init() throws InterruptedException, KeeperException,
			IOException {
		// Init GServer List
		gServerList = new HashMap<GServerInfo, ArrayList<GServerInfo>>();
		vGlobalTree = new BPlusTree<String, String>(
				SystemConf.getInstance().gServer_graph_index_global_size);
		gServerStorageMap = new HashMap<String, Integer>();
//		eGlobalTree = new BPlusTree<String, String>(
//				SystemConf.getInstance().gServer_graph_index_global_size);
		dsPathIndex = new BPlusTree<String, String>(
				SystemConf.getInstance().gServer_data_pathIndex_global_size);
		zooKeeper = new ZkObtainer().getZooKeeper();
		zooKeeper.register(zooWatcher);

		rpcThread = new Thread(new Runnable() {

			@Override
			public void run() {

				try {
					rpcServer = RPC.getServer(GMaster.this,
							SystemConf.getInstance().localIP,
							SystemConf.getInstance().RPC_GMASTER_PORT,
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
		rpcThread.start();

		List<String> children = zooKeeper.getChildren(
				SystemConf.getInstance().zoo_basePath, true);
		for (String eve : children) {
			if (!eve.equals(LockFactory.MASTER_ID)) {

				String wl;
				wl = new String(zooKeeper.getData(
						SystemConf.getInstance().zoo_basePath + "/" + eve,
						false, null));
				String[] ipv = wl.split(":");
				// ipv[0] is gServer's ip address
				if (ipv[1].equals("lock"))
					addANewServer(ipv[0]);
			}

		}
	}

	@Override
	public void run() {

		// TODO Simply output all available gServers
		try {
			init();
		} catch (InterruptedException | KeeperException e) {

			e.printStackTrace();
			return;
		} catch (IOException e) {

			e.printStackTrace();
			return;
		}
		isRunning = true;
		int loopCount = 0;
		while (isRunning == true) {
			Log_Utilities.printGMasterRuntimeLog("Running");
			
			Log_Utilities.printGMasterRuntimeLog("gServer List:");
			synchronized (gServerList) {
				Set<GServerInfo> keys = gServerList.keySet();
				for (GServerInfo key : keys) {
					Log_Utilities.printGMasterRuntimeLog("gIndexServer IP:" + key.ip);
					for (GServerInfo value : gServerList.get(key)) {
						Log_Utilities.printGMasterRuntimeLog("[" + key.ip + "] GServer"
								+ value.ip);
					}
				}
			}
			this.fileLock.checkAndRecover();

			loopCount++;

			if (loopCount == 2) {
				scanServerList(SystemConf.getInstance().zoo_basePath);
				loopCount = 0;
			}

			try {
				Thread.sleep(15000);
			} catch (InterruptedException e) {

				e.printStackTrace();
			}
		}

		// Close Connection
		try {
			rpcServer.stop();
			zooKeeper.close();
		} catch (InterruptedException e) {

			e.printStackTrace();
		}
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {

		return SystemConf.RPC_VERSION;
	}

	@Override
	public String findTargetGServer_Store(VertexInfo information) {

		if (vGlobalTree.get(information.getId()) != null) {
			// we already have this vertex
			return ErrorCode.VERTEX_ALREADYEXIST;
		}

		float maxMark = -100.0f;
		String targetServerIP = "";
		synchronized (gServerList) {
			Set<GServerInfo> keys = gServerList.keySet();
			for (GServerInfo key : keys) {
				
					Log_Utilities.printGMasterLog("MASTER", "FindTarget + Now In Keys");
				try {
					GServerProtocol proxy = RpcIOCommons
							.getGServerProtocol(key.ip);
					float mark = proxy.getMarkForTargetVertex(information);
					
						Log_Utilities.printGMasterLog("MASTER", "FindTarget + key Mark:" + mark);
					if (maxMark < mark) {
						maxMark = mark;
						targetServerIP = key.ip;
					}
					if (Debug.masterStopProxy)
						RPC.stopProxy(proxy);
					RpcIOCommons.freeGServerProtocol(key.ip);
				} catch (IOException e) {
					e.printStackTrace();
				}
				for (GServerInfo server : gServerList.get(key)) {
					
						Log_Utilities.printGMasterLog("MASTER", "FindTarget + Now In Servers");
					try {
						GServerProtocol proxy = RpcIOCommons
								.getGServerProtocol(server.ip);
						float mark = proxy.getMarkForTargetVertex(information);
						
							Log_Utilities.printGMasterLog("MASTER", "FindTarget + Servers Mark:"
									+ mark);
						if (maxMark < mark) {
							maxMark = mark;
							targetServerIP = server.ip;
						}
						if (Debug.masterStopProxy)
							RPC.stopProxy(proxy);
						RpcIOCommons.freeGServerProtocol(server.ip);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			if (targetServerIP.equals("")) {
				for (GServerInfo info : keys) {
					targetServerIP = info.ip;
					break;
				}
			}
			return targetServerIP;
		}

	}
	
	protected String findTargetGServer_Store_BatchVertex() {
		String targetIP = "";
		int currentStorageCount = Integer.MAX_VALUE;
		for (String ip : gServerStorageMap.keySet()) {
			if (targetIP.equals("")) {
				targetIP = ip;
			}
			if (currentStorageCount > gServerStorageMap.get(ip)) {
				targetIP = ip;
				currentStorageCount = gServerStorageMap.get(ip);
			}
		}
		return targetIP;
	}

	@Override
	public void stopService() {

		isRunning = false;
	}

	@Override
	public void requestToChangeIndexServer(String source_ip) {
		
			Log_Utilities.printGMasterLog("MASTER", "[" + SystemConf.getTime()
					+ "][MASTER] Request To Change Index Server! " + source_ip);

		double minUsage = Double.MAX_VALUE;
		GServerInfo targerGSInfo = new GServerInfo(source_ip);
		synchronized (gServerList) {
			for (GServerInfo server : gServerList
					.get(new GServerInfo(source_ip))) {
				try {
					GServerProtocol proxy = RpcIOCommons
							.getGServerProtocol(server.ip);
					double usage = proxy.reportUsageMark();
					if (usage < minUsage) {
						minUsage = usage;
						targerGSInfo.ip = server.ip;
					}
					if (Debug.masterStopProxy)
						RPC.stopProxy(proxy);
					RpcIOCommons.freeGServerProtocol(server.ip);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		if (!targerGSInfo.ip.equals(source_ip)) {
			// change to a new Index Server
			ArrayList<GServerInfo> array = gServerList.get(new GServerInfo(
					source_ip));
			array.remove(targerGSInfo);
			array.add(new GServerInfo(source_ip));
			targerGSInfo.isIndexServer = true;
			gServerList.remove(new GServerInfo(source_ip));
			gServerList.put(targerGSInfo, array);
			// for index server
			GServerProtocol proxy;
			try {
				proxy = RpcIOCommons.getGServerProtocol(targerGSInfo.ip);
				proxy.assignIndexServer(
						new BPlusTreeStrStrWritable(vGlobalTree),
						new BPlusTreeStrStrWritable(dsPathIndex));
				if (Debug.masterStopProxy)
					RPC.stopProxy(proxy);
				RpcIOCommons.freeGServerProtocol(targerGSInfo.ip);
				for (GServerInfo info : array) {
					proxy = RpcIOCommons.getGServerProtocol(info.ip);
					proxy.announceIndexServer(targerGSInfo.ip);
					if (Debug.masterStopProxy)
						RPC.stopProxy(proxy);
					RpcIOCommons.freeGServerProtocol(info.ip);
				}
			} catch (IOException e) {

				e.printStackTrace();
			}

		}
	}

	@Override
	public void insertVertexInfoToIndex(String vid, String ip) {
		vGlobalTree.lockWrite();
		vGlobalTree.insertOrUpdate(vid, ip);
		gServerStorageMap.put(ip, gServerStorageMap.get(ip)+1);
		vGlobalTree.unlockWrite();
		synchronized (gServerList) {
			Set<GServerInfo> keySet = gServerList.keySet();
			for (GServerInfo info : keySet) {
				if (!info.ip.equals(ip)) {
					try {
						GServerProtocol proxy = RpcIOCommons
								.getGServerProtocol(info.ip);
						proxy.putVertexInfoToIndex(vid, ip);
						if (Debug.masterStopProxy)
							RPC.stopProxy(proxy);
						RpcIOCommons.freeGServerProtocol(info.ip);
					} catch (IOException e) {

						e.printStackTrace();
					}
				}
			}
		}
	}
	
	protected void insertVertexInfoListToIndex(ArrayList<String> vids, String ip) {
		vGlobalTree.lockWrite();
		for (String id : vids) {
			vGlobalTree.insertOrUpdate(id, ip);
		}
		gServerStorageMap.put(ip, gServerStorageMap.get(ip) + vids.size());
		vGlobalTree.unlockWrite();
		synchronized (gServerList) {
			Set<GServerInfo> keySet = gServerList.keySet();
			for (GServerInfo info : keySet) {
					try {
						GServerProtocol proxy = RpcIOCommons
								.getGServerProtocol(info.ip);
						StringMapWritable writable = new StringMapWritable();
						for (String id : vids) {
							writable.data.add(new StringPairWritable(id, ip));
						}
						
						proxy.putVListToIndex(writable);
						
						if (Debug.masterStopProxy)
							RPC.stopProxy(proxy);
						RpcIOCommons.freeGServerProtocol(info.ip);
						
					} catch (IOException e) {

						e.printStackTrace();
					}
			}
		}
	}

//	@Override
//	public void insertEdgeInfoToIndex(String eid, String ip) {
//
//		eGlobalTree.insertOrUpdate(eid, ip);
//		synchronized (gServerList) {
//			Set<GServerInfo> keySet = gServerList.keySet();
//			for (GServerInfo info : keySet) {
//				if (!info.ip.equals(ip)) {
//					try {
//						GServerProtocol proxy = RpcIOCommons
//								.getGServerProtocol(info.ip);
//						proxy.putEdgeInfoToIndex(eid, ip);
//						if (Debug.masterStopProxy)
//							RPC.stopProxy(proxy);
//					} catch (IOException e) {
//
//						e.printStackTrace();
//					}
//				}
//			}
//		}
//	}

	@Override
	public void removeVertexFromIndex(String vid) {
		vGlobalTree.lockWrite();
		gServerStorageMap.put(vGlobalTree.get(vid), gServerStorageMap.get(vGlobalTree.get(vid))-1);
		vGlobalTree.remove(vid);
		vGlobalTree.unlockWrite();
		synchronized (gServerList) {
			Set<GServerInfo> keySet = gServerList.keySet();
			for (GServerInfo info : keySet) {
				try {
					GServerProtocol proxy = RpcIOCommons
							.getGServerProtocol(info.ip);
					proxy.deleteVertexFromIndex(vid);
					if (Debug.masterStopProxy)
						RPC.stopProxy(proxy);
					RpcIOCommons.freeGServerProtocol(info.ip);
				} catch (IOException e) {

					e.printStackTrace();
				}
			}
		}
	}
	
	@Override
	public String storeVertexAndEdgeList(VertexCollectionWritable vdata,
			EdgeCollectionWritable edata) {
		//store the vertex first
		//0. filter the vertex first
		VertexCollectionWritable nData = new VertexCollectionWritable();
		ArrayList<String> vids = new ArrayList<>();
		for (VertexInfo v : vdata.coll) {
			if (vGlobalTree.get(v.getId()) == null) {
				vids.add(v.getId());
				nData.coll.add(v);
			}
		}
		
		//1. update the index
		String targetIP = findTargetGServer_Store_BatchVertex();
		insertVertexInfoListToIndex(vids, targetIP);
		
		//2. send data to targetIP
		try {
			GServerProtocol proxy = RpcIOCommons.getGServerProtocol(targetIP);
			proxy.storeVertexList(nData);
			if (Debug.masterStopProxy) {
				RPC.stopProxy(proxy);
			}
			RpcIOCommons.freeGServerProtocol(targetIP);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//store the edge then
		//1. group them by source_vertex_id
		Map<String, ArrayList<EdgeInfo>> requestMap = new HashMap<>();
		for (EdgeInfo e : edata.coll) {
			String sourceV_IP = vGlobalTree.get(e.getSource_vertex_id());
			if (requestMap.get(sourceV_IP) == null) {
				requestMap.put(sourceV_IP, new ArrayList<EdgeInfo>());
			}
			requestMap.get(sourceV_IP).add(e);
		}
		for (String requestIP : requestMap.keySet()) {
			try {
				GServerProtocol proxy = RpcIOCommons.getGServerProtocol(requestIP);
				EdgeCollectionWritable ecw = new EdgeCollectionWritable(requestMap.get(requestIP));
				proxy.storeEdgeList(ecw);
				if (Debug.masterStopProxy) {
					RPC.stopProxy(proxy);
				}
				RpcIOCommons.freeGServerProtocol(requestIP);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return "";
	}

//	@Override
//	public void removeEdgeFromIndex(String eid) {
//
//		eGlobalTree.remove(eid);
//		synchronized (gServerList) {
//			Set<GServerInfo> keySet = gServerList.keySet();
//			for (GServerInfo info : keySet) {
//				try {
//					GServerProtocol proxy = RpcIOCommons
//							.getGServerProtocol(info.ip);
//					proxy.deleteEdgeFromIndex(eid);
//					if (Debug.masterStopProxy)
//						RPC.stopProxy(proxy);
//				} catch (IOException e) {
//
//					e.printStackTrace();
//				}
//			}
//		}
//	}

	@Override
	public void notifyDataSet_Insert(String source, String dsID, String hdfsPath) {
		dsPathIndex.insertOrUpdate(dsID, hdfsPath);
		synchronized (gServerList) {
			Set<GServerInfo> keySet = gServerList.keySet();
			for (GServerInfo info : keySet) {
				if (!info.ip.equals(source)) {
					try {
						GServerProtocol proxy = RpcIOCommons
								.getGServerProtocol(info.ip);
						proxy.insertDataSet_Sync(dsID, hdfsPath);
						if (Debug.masterStopProxy)
							RPC.stopProxy(proxy);
						RpcIOCommons.freeGServerProtocol(info.ip);
					} catch (IOException e) {

						e.printStackTrace();
					}
				}
			}
		}
	}

	@Override
	public void notifyDataSet_Remove(String source, String dsID) {
		dsPathIndex.remove(dsID);
		synchronized (gServerList) {
			Set<GServerInfo> keySet = gServerList.keySet();
			for (GServerInfo info : keySet) {
				if (!info.ip.equals(source)) {
					try {
						GServerProtocol proxy = RpcIOCommons
								.getGServerProtocol(info.ip);
						proxy.removeDataSet_Sync(dsID);
						if (Debug.masterStopProxy)
							RPC.stopProxy(proxy);
						RpcIOCommons.freeGServerProtocol(info.ip);
					} catch (IOException e) {

						e.printStackTrace();
					}
				}
			}
		}
	}

	@Override
	public void notifyDataSet_Index_Remove(String source, String dsID,
			String dschemaID, String attriName) {
		synchronized (gServerList) {
			Set<GServerInfo> keySet = gServerList.keySet();
			for (GServerInfo info : keySet) {
				if (!info.ip.equals(source)) {
					try {
						GServerProtocol proxy = RpcIOCommons
								.getGServerProtocol(info.ip);
						proxy.removeDSIndex_Sync(dsID, dschemaID, attriName);
						if (Debug.masterStopProxy)
							RPC.stopProxy(proxy);
						RpcIOCommons.freeGServerProtocol(info.ip);
					} catch (IOException e) {

						e.printStackTrace();
					}
				}
			}
		}
	}

	@Override
	public String findTargetGServer_StoreEdge(EdgeInfo information) {
		String targetIP = vGlobalTree.get(information.getSource_vertex_id());

		if (targetIP != null) {
			
			GServerProtocol proxy;
			try {
				proxy = RpcIOCommons.getGServerProtocol(targetIP);
				boolean result = proxy.EdgeExist(information.getId());
				RpcIOCommons.freeGServerProtocol(targetIP);
				if (result == true) {
					return ErrorCode.EDGE_ALREADYEXIST;
				} else {
					return targetIP;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		} else {
			return ErrorCode.VERTEX_NOTEXIST;
		}
	}

	@Override
	public String createDSIndex(String dsID, String dschemaID, String attriName) {
		double minUsage = Double.MAX_VALUE;
		String targetIP = null;

		try {
			synchronized (gServerList) {
				Set<GServerInfo> keySet = gServerList.keySet();
				for (GServerInfo key : keySet) {
					for (GServerInfo node : gServerList.get(key)) {
						GServerProtocol proxy = RpcIOCommons
								.getGServerProtocol(node.ip);
						double usage = proxy.reportUsageMark();
						if (usage < minUsage) {
							minUsage = usage;
							targetIP = node.ip;
						}
						RpcIOCommons.freeGServerProtocol(node.ip);
					}
				}

			}
			if (targetIP != null) {
				GServerProtocol proxy = RpcIOCommons
						.getGServerProtocol(targetIP);
				String result = proxy.createDSIndex(dsID, dschemaID, attriName);
				RpcIOCommons.freeGServerProtocol(targetIP);
				return result;
			}
		} catch (IOException e) {
			return e.getLocalizedMessage();
		}
		return "ERROR: Can't find a targetServer";
	}
}
