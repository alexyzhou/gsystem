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

	protected static final String MASTER_IP = "gServer.master.ip";

	// For Zoo
	protected static final String ZOO_BASEPATH = "zoo.basepath";
	protected static final String ZOO_BASEPATH_GSCHEMA = "zoo.basepath.gschema";
	protected static final String ZOO_BASEPATH_DSCHEMA = "zoo.basepath.dSchema";
	protected static final String ZOO_URL = "zoo.url";

	// For Buffer_Size and Index_MAXSize
	protected static final String GSERVER_GRAPH_RBUFFER_EDGE_SIZE = "gServer.graph.buffer.r.edge";
	protected static final String GSERVER_GRAPH_RBUFFER_VERTEX_SIZE = "gServer.graph.buffer.r.vertex";
	protected static final String GSERVER_GRAPH_WBUFFER_EDGE_SIZE = "gServer.graph.buffer.w.edge";
	protected static final String GSERVER_GRAPH_WBUFFER_VERTEX_SIZE = "gServer.graph.buffer.w.vertex";

	protected static final String GSERVER_GRAPH_BUFFER_SCHEMA_SIZE = "gServer.graph.buffer.schema";

	protected static final String GSERVER_GRAPH_GLOBALINDEX_SIZE = "gServer.graph.index.global";
	protected static final String GSERVER_GRAPH_LOCALINDEX_SIZE = "gServer.graph.index.local";

	protected static final String GSERVER_DATA_PATHINDEX_GLOBAL_SIZE = "gServer.data.pathIndex.global";
	protected static final String GSERVER_DATA_BUFFER_INDEX_SIZE = "gServer.data.buffer.index";
	protected static final String GSERVER_DATA_BUFFER_SCHEMA_SIZE = "gServer.data.buffer.schema";

	// For MapReduce JARs
	protected static final String GSERVER_DATA_INDEX_SETUP_JARPATH = "gServer.data.index.setup.jarpath";

	// For Usage Threshold
	protected static final String GSERVER_MAX_USAGE_CPU = "gServer.usage.cpu";
	protected static final String GSERVER_MAX_USAGE_MEM = "gServer.usage.mem";

	// For HDFS
	protected static final String HDFS_BASEPATH = "hdfs.basepath";
	protected static final String HDFS_BASEPATH_DATA_INDEX = "hdfs.basepath.data.index";
	protected static final String HDFS_TEMPPATH_DATA_INDEX = "hdfs.temppath.data.index";

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// Check Environment Variables
		if (System.getenv("HADOOP_HOME") == null || System.getenv("HADOOP_PREFIX") == null) {
			System.out.println("[ERROR] $HADOOP_HOME or $HADOOP_PREFIX is not set!");
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
				SystemConf.getInstance().gServer_graph_rBuffer_edge_size = new Integer(
						prop.getProperty(GSERVER_GRAPH_RBUFFER_EDGE_SIZE))
						.intValue();
				SystemConf.getInstance().gServer_graph_rBuffer_vertex_size = new Integer(
						prop.getProperty(GSERVER_GRAPH_RBUFFER_VERTEX_SIZE))
						.intValue();
				SystemConf.getInstance().gServer_graph_wBuffer_edge_size = new Integer(
						prop.getProperty(GSERVER_GRAPH_WBUFFER_EDGE_SIZE))
						.intValue();
				SystemConf.getInstance().gServer_graph_wBuffer_vertex_size = new Integer(
						prop.getProperty(GSERVER_GRAPH_WBUFFER_VERTEX_SIZE))
						.intValue();
				SystemConf.getInstance().gServer_graph_buffer_schema_size = new Integer(
						prop.getProperty(GSERVER_GRAPH_BUFFER_SCHEMA_SIZE))
						.intValue();
				SystemConf.getInstance().gServer_graph_index_global_size = new Integer(
						prop.getProperty(GSERVER_GRAPH_GLOBALINDEX_SIZE))
						.intValue();
				SystemConf.getInstance().gServer_graph_index_local_size = new Integer(
						prop.getProperty(GSERVER_GRAPH_LOCALINDEX_SIZE))
						.intValue();
				SystemConf.getInstance().gServer_data_pathIndex_global_size = new Integer(
						prop.getProperty(GSERVER_DATA_PATHINDEX_GLOBAL_SIZE))
						.intValue();
				SystemConf.getInstance().gServer_data_buffer_index_size = new Integer(
						prop.getProperty(GSERVER_DATA_BUFFER_INDEX_SIZE))
						.intValue();
				SystemConf.getInstance().gServer_data_buffer_schema_size = new Integer(
						prop.getProperty(GSERVER_DATA_BUFFER_SCHEMA_SIZE))
						.intValue();
				SystemConf.getInstance().gServer_data_index_setup_jarPath = prop
						.getProperty(GSERVER_DATA_INDEX_SETUP_JARPATH);
				SystemConf.getInstance().gServer_usage_cpu = new Integer(
						prop.getProperty(GSERVER_MAX_USAGE_CPU));
				SystemConf.getInstance().gServer_usage_mem = new Integer(
						prop.getProperty(GSERVER_MAX_USAGE_MEM));
				SystemConf.getInstance().hdfs_basePath = prop
						.getProperty(HDFS_BASEPATH);
				SystemConf.getInstance().hdfs_basePath_data_index = prop
						.getProperty(HDFS_BASEPATH_DATA_INDEX);
				SystemConf.getInstance().hdfs_tempPath_data_index = prop
						.getProperty(HDFS_TEMPPATH_DATA_INDEX);

				SystemConf.getInstance().zoo_basePath = prop
						.getProperty(ZOO_BASEPATH);
				SystemConf.getInstance().zoo_basePath_gSchema = prop
						.getProperty(ZOO_BASEPATH_GSCHEMA);
				SystemConf.getInstance().zoo_basePath_dSchema = prop
						.getProperty(ZOO_BASEPATH_DSCHEMA);
				SystemConf.getInstance().zoo_url = prop.getProperty(ZOO_URL);
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
