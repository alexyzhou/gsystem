package zk;

import java.util.Collections;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import system.SystemConf;

public class LockFactory {

	private static LockFactory _instance = null;

	public static LockFactory getInstance() throws InterruptedException, KeeperException {
		if (_instance == null) {
			_instance = new LockFactory();
			_instance.init();
		}
		return _instance;
	}

	private void init() throws InterruptedException, KeeperException {
		DEFAULT_ZOOKEEPER = new ZkObtainer().getZooKeeper();
		
		//check ZooBasepath
		if (DEFAULT_ZOOKEEPER.exists(SystemConf.getInstance().zoo_basePath, false) == null) {
			DEFAULT_ZOOKEEPER.create(SystemConf.getInstance().zoo_basePath, new String("").getBytes(),
					Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}

	public ZooKeeper DEFAULT_ZOOKEEPER;


	public static final String MASTER_ID = "master";

	// data格式: ip:stat 如: 10.232.35.70:lock 10.232.35.70:unlock
	public synchronized Lock getLock(String ip, boolean isMaster)
			throws Exception {
		if (DEFAULT_ZOOKEEPER != null) {
			String path;
			if (isMaster == false) {
				path = SystemConf.getInstance().zoo_basePath + "/" + ip;
			} else {
				path = SystemConf.getInstance().zoo_basePath + "/" + MASTER_ID;
			}

			Stat stat = null;
			try {
				stat = DEFAULT_ZOOKEEPER.exists(path, false);
			} catch (Exception e) {
				// TODO: use log system and throw new exception
				throw e;
			}
			if (stat != null) {
				byte[] data = DEFAULT_ZOOKEEPER.getData(path, null, stat);
				String dataStr = new String(data);
				String[] ipv = dataStr.split(":");
				if (ip.equals(ipv[0])) {
					Lock lock = new Lock(path);
					lock.setZooKeeper(DEFAULT_ZOOKEEPER);
					return lock;
				}
				// is not your lock, return null
				else {
					return null;
				}
			}
			// no lock created yet, you can get it
			else {
				createZnode(path,ip);
				Lock lock = new Lock(path);
				lock.setZooKeeper(DEFAULT_ZOOKEEPER);
				return lock;
			}
		}
		return null;
	}

	private void createZnode(String path, String ip) throws Exception {

		if (DEFAULT_ZOOKEEPER != null) {
			
			String data = ip + ":unlock";
			DEFAULT_ZOOKEEPER.create(path, data.getBytes(), Collections
					.singletonList(new ACL(Perms.ALL, Ids.ANYONE_ID_UNSAFE)),
					CreateMode.EPHEMERAL);
		}
	}
}