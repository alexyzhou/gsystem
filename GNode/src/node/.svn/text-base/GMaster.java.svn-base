package node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import test.Debug;
import zk.Lock;
import zk.LockFactory;
import zk.ZkObtainer;
import data.io.EdgeInfo;
import data.io.VertexInfo;
import data.writable.BPlusTreeStrStrWritable;
import data.writable.StringMapWritable;
import data.writable.StringPairWritable;
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
		public boolean equals(Object obj) {
			// TODO Auto-generated method stub
			if (obj instanceof GServerInfo) {
				if (((GServerInfo) obj).ip.equals(this.ip))
					return true;
			}
			return false;
		}

	}

	protected HashMap<GServerInfo, ArrayList<GServerInfo>> gServerList;
	protected BPlusTree<String, String> vGlobalTree;
	protected BPlusTree<String, String> eGlobalTree;
	protected BPlusTree<String, String> dsPathIndex;

	protected StringMapWritable getVList() {
		StringMapWritable smw = new StringMapWritable();
		Set<String> keySet = vGlobalTree.getKeySet();
		for (String key : keySet) {
			smw.data.add(new StringPairWritable(key, vGlobalTree.get(key)));
		}
		return smw;
	}

	protected StringMapWritable getEList() {
		StringMapWritable smw = new StringMapWritable();
		Set<String> keySet = eGlobalTree.getKeySet();
		for (String key : keySet) {
			smw.data.add(new StringPairWritable(key, eGlobalTree.get(key)));
		}
		return smw;
	}

	protected Watcher zooWatcher = new Watcher() {

		@Override
		public void process(WatchedEvent event) {
			// TODO Auto-generated method stub
			if (event.getType() == EventType.NodeChildrenChanged) {
				// Serverlist changed!
				System.out.println("[MASTER] NodeChildrenChanged!");
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
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected void addANewServer(String ip) {
		// TO DO
		System.out.println("[MASTER] AddaNewServer Called!");
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
						return;
					} catch (IOException e) {
						// TODO Auto-generated catch block
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
						new BPlusTreeStrStrWritable(eGlobalTree),
						new BPlusTreeStrStrWritable(dsPathIndex));
				if (Debug.masterStopProxy)
					RPC.stopProxy(proxy);
				return;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	protected void removeDeadServer(GServerInfo ip) {
		// TO DO
		System.out.println("[MASTER] removeDeadServer Called!");
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
										new BPlusTreeStrStrWritable(eGlobalTree),
										new BPlusTreeStrStrWritable(dsPathIndex));
							} else {
								proxy.announceIndexServer(targetIP.ip);
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
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
				SystemConf.GSERVER_GLOBALINDEXTREE_FACTOR);
		eGlobalTree = new BPlusTree<String, String>(
				SystemConf.GSERVER_GLOBALINDEXTREE_FACTOR);
		dsPathIndex = new BPlusTree<String, String>(
				SystemConf.DATASET_PATHINDEX_FACTOR);
		zooKeeper = new ZkObtainer().getZooKeeper();
		zooKeeper.register(zooWatcher);

		rpcThread = new Thread(new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				try {
					rpcServer = RPC.getServer(GMaster.this,
							SystemConf.getInstance().localIP,
							SystemConf.getInstance().RPC_GMASTER_PORT,
							new Configuration());
					rpcServer.start();
					rpcServer.join();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		});
		rpcThread.start();

		List<String> children = zooKeeper.getChildren(
				SystemConf.getInstance().zoo_gnode_base_path, true);
		for (String eve : children) {
			if (!eve.equals(LockFactory.MASTER_ID)) {

				String wl;
				wl = new String(zooKeeper.getData(
						SystemConf.getInstance().zoo_gnode_base_path + "/"
								+ eve, false, null));
				String[] ipv = wl.split(":");
				// ipv[0] is gServer's ip address
				if (ipv[1].equals("lock"))
					addANewServer(ipv[0]);
			}

		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		// TODO Simply output all available gServers
		try {
			init();
		} catch (InterruptedException | KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		isRunning = true;
		int loopCount = 0;
		while (isRunning == true) {
			System.out
					.println("[" + SystemConf.getTime() + "][MASTER] Running");
			System.out.println("[" + SystemConf.getTime()
					+ "][MASTER] gServer List:");
			synchronized (gServerList) {
				Set<GServerInfo> keys = gServerList.keySet();
				for (GServerInfo key : keys) {
					System.out.println("[" + SystemConf.getTime()
							+ "][MASTER] gIndexServer IP:" + key.ip);
					for (GServerInfo value : gServerList.get(key)) {
						System.out.println("[" + SystemConf.getTime()
								+ "][MASTER][" + key.ip + "] GServer"
								+ value.ip);
					}
				}
			}
			this.fileLock.checkAndRecover();

			loopCount++;

			if (loopCount == 2) {
				scanServerList(SystemConf.getInstance().zoo_gnode_base_path);
				loopCount = 0;
			}

			try {
				Thread.sleep(15000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// Close Connection
		try {
			rpcServer.stop();
			zooKeeper.close();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		return SystemConf.RPC_VERSION;
	}

	@Override
	public String findTargetGServer_Store(VertexInfo information) {
		// TODO Auto-generated method stub
		float maxMark = -100.0f;
		String targetServerIP = "";
		synchronized (gServerList) {
			Set<GServerInfo> keys = gServerList.keySet();
			for (GServerInfo key : keys) {
				if (Debug.findTargetGServer_Store)
					System.out.println("[" + SystemConf.getTime()
							+ "][MASTER] FindTarget + Now In Keys");
				try {
					GServerProtocol proxy = RpcIOCommons
							.getGServerProtocol(key.ip);
					float mark = proxy.getMarkForTargetVertex(information);
					if (Debug.findTargetGServer_Store)
						System.out.println("[" + SystemConf.getTime()
								+ "][MASTER] FindTarget + key Mark:" + mark);
					if (maxMark < mark) {
						maxMark = mark;
						targetServerIP = key.ip;
					}
					if (Debug.masterStopProxy)
						RPC.stopProxy(proxy);
				} catch (IOException e) {
					e.printStackTrace();
				}
				for (GServerInfo server : gServerList.get(key)) {
					if (Debug.findTargetGServer_Store)
						System.out.println("[" + SystemConf.getTime()
								+ "][MASTER] FindTarget + Now In Servers");
					try {
						GServerProtocol proxy = RpcIOCommons
								.getGServerProtocol(server.ip);
						float mark = proxy.getMarkForTargetVertex(information);
						if (Debug.findTargetGServer_Store)
							System.out.println("[" + SystemConf.getTime()
									+ "][MASTER] FindTarget + Servers Mark:"
									+ mark);
						if (maxMark < mark) {
							maxMark = mark;
							targetServerIP = server.ip;
						}
						if (Debug.masterStopProxy)
							RPC.stopProxy(proxy);
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

	@Override
	public void stopService() {
		// TODO Auto-generated method stub
		isRunning = false;
	}

	@Override
	public void requestToChangeIndexServer(String source_ip) {
		// TODO Auto-generated method stub
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
			gServerList.put(targerGSInfo, array);
			// for index server
			GServerProtocol proxy;
			try {
				proxy = RpcIOCommons.getGServerProtocol(targerGSInfo.ip);
				proxy.assignIndexServer(
						new BPlusTreeStrStrWritable(vGlobalTree),
						new BPlusTreeStrStrWritable(eGlobalTree),
						new BPlusTreeStrStrWritable(dsPathIndex));
				if (Debug.masterStopProxy)
					RPC.stopProxy(proxy);
				for (GServerInfo info : array) {
					proxy = RpcIOCommons.getGServerProtocol(info.ip);
					proxy.announceIndexServer(targerGSInfo.ip);
					if (Debug.masterStopProxy)
						RPC.stopProxy(proxy);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	@Override
	public void insertVertexInfoToIndex(String vid, String ip) {
		// TODO Auto-generated method stub
		vGlobalTree.insertOrUpdate(vid, ip);
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
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}

	@Override
	public void insertEdgeInfoToIndex(String eid, String ip) {
		// TODO Auto-generated method stub
		eGlobalTree.insertOrUpdate(eid, ip);
		synchronized (gServerList) {
			Set<GServerInfo> keySet = gServerList.keySet();
			for (GServerInfo info : keySet) {
				if (!info.ip.equals(ip)) {
					try {
						GServerProtocol proxy = RpcIOCommons
								.getGServerProtocol(info.ip);
						proxy.putEdgeInfoToIndex(eid, ip);
						if (Debug.masterStopProxy)
							RPC.stopProxy(proxy);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}

	@Override
	public void removeVertexFromIndex(String vid) {
		vGlobalTree.remove(vid);
		synchronized (gServerList) {
			Set<GServerInfo> keySet = gServerList.keySet();
			for (GServerInfo info : keySet) {
				try {
					GServerProtocol proxy = RpcIOCommons
							.getGServerProtocol(info.ip);
					proxy.deleteVertexFromIndex(vid);
					if (Debug.masterStopProxy)
						RPC.stopProxy(proxy);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void removeEdgeFromIndex(String eid) {
		// TODO Auto-generated method stub
		eGlobalTree.remove(eid);
		synchronized (gServerList) {
			Set<GServerInfo> keySet = gServerList.keySet();
			for (GServerInfo info : keySet) {
				try {
					GServerProtocol proxy = RpcIOCommons
							.getGServerProtocol(info.ip);
					proxy.deleteEdgeFromIndex(eid);
					if (Debug.masterStopProxy)
						RPC.stopProxy(proxy);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

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
					} catch (IOException e) {
						// TODO Auto-generated catch block
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
					} catch (IOException e) {
						// TODO Auto-generated catch block
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
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}

	@Override
	public String findTargetGServer_StoreEdge(EdgeInfo information) {
		if (vGlobalTree.get(information.getTarget_vertex_id()) != null) {
			return vGlobalTree.get(information.getSource_vertex_id());
		} else {
			return null;
		}
	}

}
