package zk;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;

import system.SystemConf;

public class ZkObtainer {
	
	private CountDownLatch connectionLatch = new CountDownLatch(1);
	private static final int SESSION_TIMEOUT = 180000;
	private boolean connectIndicator;
	
	private Watcher Watcher = new Watcher() {
		public void process(WatchedEvent event) {
			// 节点的事件处理. you can do something when the node's data change
			System.out.println("event " + event.getType() + " has happened!");
			if (event.getState() == KeeperState.SyncConnected
					&& connectIndicator == false) {
				System.out.println("[Count Down!]");
				connectionLatch.countDown(); // unlock
				connectIndicator = true;
			}
		}
	};
	
	public ZooKeeper getZooKeeper() throws InterruptedException {
		try {
			connectIndicator = false;
			ZooKeeper zooKeeper = new ZooKeeper(SystemConf.getInstance().zoo_url,
					ZkObtainer.SESSION_TIMEOUT, this.Watcher);
			if (States.CONNECTING == zooKeeper.getState()) {
				connectionLatch.await();
			}
			return zooKeeper;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
}
