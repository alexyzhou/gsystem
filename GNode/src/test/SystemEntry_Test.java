package test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Properties;

import system.SystemConf;
import node.GMaster;
import node.GServer;
import zk.Lock;
import zk.LockFactory;

public class SystemEntry_Test {

	protected static final String MASTER_IP = "server.master.ip";

	protected static final String ZOO_BASEPATH = "zoo.basepath";

	// For BufferPool
	protected static final String GSERVER_MAX_RBUFFER_EDGE = "gServer.buffer.r.edge";
	protected static final String GSERVER_MAX_RBUFFER_VERTEX = "gServer.buffer.r.vertex";
	protected static final String GSERVER_MAX_WBUFFER_EDGE = "gServer.buffer.w.edge";
	protected static final String GSERVER_MAX_WBUFFER_VERTEX = "gServer.buffer.w.vertex";

	// For HDFS
	protected static final String HDFS_BASEPATH = "hdfs.basepath";

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// Check Environment Variables
		if (System.getenv("HADOOP_HOME") == null) {
			System.out.println("[ERROR] $HADOOP_HOME is not set!");
			return;
		}

		try {
			systemInit();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static Thread masterThread;
	private static ArrayList<Thread> gServerThreads = new ArrayList<Thread>();

	private static void systemInit() throws InterruptedException,
			FileNotFoundException {

		Properties prop = new Properties();
		InputStream in = new FileInputStream("systemconf.properties");
		try {
			prop.load(in);

			// properties load begin
			{
				SystemConf.getInstance().masterIP = prop.getProperty(MASTER_IP);
				SystemConf.getInstance().hdfs_basePath = prop
						.getProperty(HDFS_BASEPATH);
				SystemConf.getInstance().zoo_gnode_base_path = prop
						.getProperty(ZOO_BASEPATH);
				SystemConf.GSERVER_MAX_RBUFFER_EDGE = new Integer(
						prop.getProperty(GSERVER_MAX_RBUFFER_EDGE)).intValue();
				SystemConf.GSERVER_MAX_RBUFFER_VERTEX = new Integer(
						prop.getProperty(GSERVER_MAX_RBUFFER_VERTEX))
						.intValue();
				SystemConf.GSERVER_MAX_WBUFFER_EDGE = new Integer(
						prop.getProperty(GSERVER_MAX_WBUFFER_EDGE)).intValue();
				SystemConf.GSERVER_MAX_WBUFFER_VERTEX = new Integer(
						prop.getProperty(GSERVER_MAX_WBUFFER_VERTEX))
						.intValue();
			}
			// properties load finished

			LockFactory lockFactory = LockFactory.getInstance();

			// Am I a MasterServer?
			InetAddress address = InetAddress.getLocalHost();
			SystemConf.getInstance().localIP = address.getHostAddress();

			// Trying to launch 1 master, 2 slaves

			// MASTER
			launchMaster(lockFactory);

			// Slaves
			for (int i = 0; i < 2; i++) {
				launchGServer(lockFactory, i);
				Thread.sleep(2000);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
	}

	protected static void launchMaster(LockFactory lockFactory)
			throws Exception {
		Lock lock = lockFactory.getLock(SystemConf.getInstance().localIP, true);
		while (lock == null) {
			// I cannot get the lock
			System.err.println("[ERROR]" + SystemConf.getInstance().localIP
					+ " cannot get master's lock.");
			System.err.println("[ERROR]" + SystemConf.getInstance().masterIP
					+ " holds the master's lock.");
			System.err.println("[ERROR]" + SystemConf.getInstance().localIP
					+ " trying to obtain master's lock in 10 seconds.");
			Thread.sleep(10000);
			lock = lockFactory.getLock(SystemConf.getInstance().localIP, true);
		}
		// Successfully obtained the lock
		// SystemConsticf.getInstance().isMaster = true;
		// trying to lock the file
		if (lock.isLock()) {
			lock.lock(SystemConf.getInstance().localIP);
		}
		masterThread = new Thread(new GMaster(lock, InetAddress.getLocalHost()
				.getHostAddress()));
		masterThread.start();
	}

	protected static void launchGServer(LockFactory lockFactory, int count)
			throws Exception {

		String ipString = "192.168.0." + count;

		Lock lock = lockFactory.getLock(ipString, false);

		if (lock == null) {
			// I cannot get the lock
			System.err.println("[ERROR]" + ipString
					+ " cannot get the lock. App will exit.");
			return;
		} else {
			// Successfully obtained the lock
			// SystemConf.getInstance().isMaster = false;
			// trying to lock the file
			if (!lock.isLock(ipString)) {
				lock.lock(ipString);
			}
			Thread slaveThread = new Thread(new GServer(lock, ipString));
			slaveThread.start();
			gServerThreads.add(slaveThread);
		}
	}

}
