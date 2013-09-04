package myCode;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

public class ServerRunnable implements Runnable {

	public static int MAX_ITERATION = 20;
	public static int SLEEP_INTERVAL = 500;
	public static int VOTE_MAX_TIME = 5000;

	private static String ELECT_Path = "/election";
	private static String VOTE_Path = "/election/vote";

	private static final int SESSION_TIMEOUT = 180000;

	protected ZooKeeper zk;
	protected CountDownLatch connectionLatch;
	protected Watcher wh = new Watcher() {

		@Override
		public void process(WatchedEvent event) {
			// TODO Auto-generated method stub

			System.out.println(event.toString());

			if (event.getState() == KeeperState.SyncConnected
					&& connected == false) {
				connectionLatch.countDown(); // unlock
				connected = true;
			}

			if (selectionFinished == false) {

				if (event.getType() == EventType.NodeChildrenChanged) {
					suspend = true;
					//System.err.println("Thread " + id + "Suspend!");
					long bt = new Date().getTime();
					try {
						List<String> children = zk.getChildren(event.getPath(),
								true);
						if (event.getPath().equals(VOTE_Path)) {
							for (String eve : children) {
								String wl;
								try {
									wl = new String(zk.getData(event.getPath()
											+ "/" + eve, false, null));
									if (Integer.parseInt(wl) < workload) {
										isLeader = false;
									}
								} catch (KeeperException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						} else if (event.getPath().equals(ELECT_Path)) {
							for (String eve : children) {
								if (eve.contains("leader")) {
									String vlId;
									try {
										vlId = new String(zk.getData(
												event.getPath() + "/" + eve,
												false, null));
										System.out.println("Thread<" + id
												+ "> Leader [" + vlId
												+ "] Election Finish!");
										selectionFinished = true;
									} catch (KeeperException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									} catch (InterruptedException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
									
									break;
								}
							}
						}

					} catch (KeeperException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					elapsedTime += new Date().getTime() - bt;
					suspend = false;
					//System.err.println("Thread " + id + "Resumed!");
				}

			}

		}

	};

	protected int id;
	protected int workload;

	protected boolean connected;
	protected boolean selectionFinished;
	protected boolean suspend;
	protected long elapsedTime;
	protected long beginTime;
	protected boolean isLeader;

	public ServerRunnable(int id) {
		this.id = id;
		workload = getRandomValueTo100();
		System.out.println("[" + id + "]Workload" + workload);

		selectionFinished = false;
		beginTime = -1;
		isLeader = false;

		suspend = false;
		elapsedTime = 0;

		connected = false;

		try {
			connectionLatch = new CountDownLatch(1);
			zk = new ZooKeeper("localhost:2181", this.SESSION_TIMEOUT, this.wh);
			if (States.CONNECTING == zk.getState()) {
				connectionLatch.await();
			}
			if (zk.exists(ServerRunnable.ELECT_Path, false) == null) {
				zk.create(this.ELECT_Path, new String("").getBytes(),
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			if (zk.exists(ServerRunnable.VOTE_Path, false) == null) {
				zk.create(this.VOTE_Path, new String("").getBytes(),
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected int getRandomValueTo10() {
		return (int) Math.round(Math.random() * (20 - 0) - 10);
	}

	protected int getRandomValueTo100() {
		return (int) Math.round(Math.random() * (100 - 0) + 0);
	}

	protected void becomeLeader() throws KeeperException, InterruptedException {
		zk.create(this.VOTE_Path + "/" + this.id,
				Integer.toString(this.workload).getBytes(),
				Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT);
		this.isLeader = true;
		this.beginTime = new Date().getTime();
	}

	protected void announceLeader() throws KeeperException,
			InterruptedException {
		zk.create(this.ELECT_Path + "/leader", Integer.toString(this.id)
				.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("Thread<" + this.id + "> Leader [" + this.id
				+ "] Election Finish!");

	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

		// Initial vote

		try {

			List<String> children = zk.getChildren(this.ELECT_Path, true);

			for (String child : children) {
				if (child.contains("leader")) {
					String vlId = new String(zk.getData(this.VOTE_Path + "/"
							+ child, false, null));
					System.out.println("Thread<" + this.id + "> Leader ["
							+ vlId + "] Election Finish!");
					return;
				}
			}

			children = zk.getChildren(this.VOTE_Path, true);

			if (children.size() == 0) {
				becomeLeader();
			} else {
				this.isLeader = true;
				for (String child : children) {

					String wl = new String(zk.getData(this.VOTE_Path + "/"
							+ child, false, null));
					if (Integer.parseInt(wl) < this.workload) {
						this.isLeader = false;
					}
				}
				if (this.isLeader == true) {
					becomeLeader();
				}
			}

			while (true) {
				//System.out.println("Thread "+this.id + "sl" + suspend);
				if (suspend == true) {
					Thread.sleep(100);
				} else {
					if (((this.isLeader == true) && (new Date().getTime()
							- this.beginTime < ServerRunnable.VOTE_MAX_TIME
							+ elapsedTime))
							|| ((this.isLeader == false) && (selectionFinished == false))) {
						Thread.sleep(SLEEP_INTERVAL);
						
					} else {
						break;
					}
				}
			}

			if (this.isLeader == true) {
				selectionFinished = true;
				announceLeader();
			}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				zk.close();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return;
		}

		try {
			zk.close();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}

}
