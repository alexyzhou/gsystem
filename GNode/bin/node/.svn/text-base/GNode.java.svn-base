package node;

import org.apache.hadoop.ipc.RPC.Server;
import org.apache.zookeeper.ZooKeeper;

import zk.Lock;

public abstract class GNode implements Runnable {
	
	protected Lock fileLock;
	protected boolean isRunning;
	protected ZooKeeper zooKeeper;
	
	protected Server rpcServer;
	protected Thread rpcThread;
	
	protected String ip;
	
	public GNode(Lock lock, String ip) throws Exception {
		this.fileLock = lock;
		this.ip = ip;
		if (!this.fileLock.isLock()) {
			this.fileLock.lock(ip);
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

}
