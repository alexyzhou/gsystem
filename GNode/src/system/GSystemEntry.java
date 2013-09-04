package system;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import node.GMaster;
import node.GServer;
import zk.Lock;
import zk.LockFactory;

public class GSystemEntry {

	protected static final String MASTER_IP = "server.master.ip";

	protected static final String ZOO_BASEPATH = "zoo.basepath";
	protected static final String ZOO_SCHEMA_BASEPATH = "zoo.basepath.gschema";
	protected static final String ZOO_URL = "zoo.url";

	// For BufferPool
	protected static final String GSERVER_MAX_RBUFFER_EDGE = "gServer.buffer.r.edge";
	protected static final String GSERVER_MAX_RBUFFER_VERTEX = "gServer.buffer.r.vertex";
	protected static final String GSERVER_MAX_WBUFFER_EDGE = "gServer.buffer.w.edge";
	protected static final String GSERVER_MAX_WBUFFER_VERTEX = "gServer.buffer.w.vertex";

	protected static final String GSERVER_MAX_SCHEMACACHE = "gServer.buffer.schema";

	protected static final String GSERVER_MAX_GLOBALINDEX_FACTOR = "gServer.buffer.index.global";
	protected static final String GSERVER_MAX_LOCALINDEX_FACTOR = "gServer.buffer.index.local";
	protected static final String GSERVER_MAX_USAGE_CPU = "gServer.usage.cpu";
	protected static final String GSERVER_MAX_USAGE_MEM = "gServer.usage.mem";

	// For HDFS
	protected static final String HDFS_BASEPATH = "hdfs.basepath";
	
	// For DataLayer
	protected static final String DATASET_PATHINDEX_FACTOR = "dataSet.pathIndex.global";
	protected static final String HDFS_DSINDEX_BASEPATH = "hdfs.dataset.index.basepath";
	protected static final String DATASET_INDEX_CACHE_FACTOR = "dataSet.buffer.index.factor";
	protected static final String ZOO_DS_SCHEMA_BASEPATH = "zoo.basepath.dsSchema";
	protected static final String GSERVER_MAX_DS_SCHEMA_CACHE = "gServer.buffer.schema.ds";

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
				SystemConf.getInstance().zoo_gp_schema_basePath = prop
						.getProperty(ZOO_SCHEMA_BASEPATH);
				SystemConf.getInstance().zoo_gnode_base_path = prop
						.getProperty(ZOO_BASEPATH);
				SystemConf.getInstance().zoo_gp_schema_basePath = prop
						.getProperty(ZOO_SCHEMA_BASEPATH);
				SystemConf.getInstance().zoo_url = prop.getProperty(ZOO_URL);
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
				SystemConf.GSERVER_MAX_GP_SCHEMA_CACHE = new Integer(
						prop.getProperty(GSERVER_MAX_SCHEMACACHE));
				SystemConf.GSERVER_GLOBALINDEXTREE_FACTOR = new Integer(
						prop.getProperty(GSERVER_MAX_GLOBALINDEX_FACTOR));
				SystemConf.GSERVER_LOCALINDEXTREE_FACTOR = new Integer(
						prop.getProperty(GSERVER_MAX_LOCALINDEX_FACTOR));
				SystemConf.GSERVER_MAXUSAGE_CPU = new Integer(
						prop.getProperty(GSERVER_MAX_USAGE_CPU));
				SystemConf.GSERVER_MAXUSAGE_MEM = new Integer(
						prop.getProperty(GSERVER_MAX_USAGE_MEM));
				SystemConf.DATASET_PATHINDEX_FACTOR = new Integer(
						prop.getProperty(DATASET_PATHINDEX_FACTOR));
				SystemConf.getInstance().hdfs_dsIndex_basePath = prop.getProperty(HDFS_DSINDEX_BASEPATH);
				SystemConf.DATASET_INDEX_CACHE_FACTOR = new Integer(
						prop.getProperty(DATASET_INDEX_CACHE_FACTOR));
				SystemConf.getInstance().zoo_ds_schema_basePath = prop.getProperty(ZOO_DS_SCHEMA_BASEPATH);
				SystemConf.DATASET_SCHEMA_CACHE_FACTOR = new Integer(
						prop.getProperty(GSERVER_MAX_DS_SCHEMA_CACHE));
			}
			// properties load finished

			LockFactory lockFactory = LockFactory.getInstance();

			// Am I a MasterServer?
			InetAddress address = InetAddress.getLocalHost();
			SystemConf.getInstance().localIP = address.getHostAddress();

			System.err.println("[LocalIP]" + SystemConf.getInstance().localIP);
			System.err
					.println("[MasterIP]" + SystemConf.getInstance().masterIP);

			if (SystemConf.getInstance().localIP.equals(SystemConf
					.getInstance().masterIP)) {
				// I am a MasterServer
				// trying to obtain Master Lock
				System.err.println("I am a Master!");
				Lock lock = lockFactory.getLock(
						SystemConf.getInstance().localIP, true);

				while (lock == null) {
					// I cannot get the lock
					System.err.println("[ERROR]"
							+ SystemConf.getInstance().localIP
							+ " cannot get master's lock.");
					System.err.println("[ERROR]"
							+ SystemConf.getInstance().masterIP
							+ " holds the master's lock.");
					System.err.println("[ERROR]"
							+ SystemConf.getInstance().localIP
							+ " trying to obtain master's lock in 10 seconds.");
					Thread.sleep(10000);
					lock = lockFactory.getLock(
							SystemConf.getInstance().localIP, true);
				}
				// Successfully obtained the lock
				SystemConf.getInstance().isMaster = true;
				// trying to lock the file
				if (lock.isLock()) {
					lock.lock(SystemConf.getInstance().localIP);
				}
				SystemConf.getInstance().serviceThread = new Thread(
						new GMaster(lock, InetAddress.getLocalHost()
								.getHostAddress()));
				SystemConf.getInstance().serviceThread.start();

			} else {
				// I am a DataServer
				// trying to obtain DataServer Lock
				System.err.println("I am a DServer!");
				SystemConf.getInstance().isIndexServer = new Boolean(false);
				Lock lock = lockFactory.getLock(
						SystemConf.getInstance().localIP, false);

				if (lock == null) {
					// I cannot get the lock
					System.err.println("[ERROR]"
							+ SystemConf.getInstance().localIP
							+ " cannot get the lock. App will exit.");
					return;
				} else {
					// Successfully obtained the lock
					SystemConf.getInstance().isMaster = false;
					// trying to lock the file
					if (lock.isLock()) {
						lock.lock(SystemConf.getInstance().localIP);
					}
					SystemConf.getInstance().serviceThread = new Thread(
							new GServer(lock, InetAddress.getLocalHost()
									.getHostAddress()));
					SystemConf.getInstance().serviceThread.start();
				}
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

}
