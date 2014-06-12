package system;

import java.text.SimpleDateFormat;
import java.util.Date;

public class SystemConf {
	
	protected static SystemConf instance = null;
	
	public static SystemConf getInstance() {
		if (instance == null) {
			instance = new SystemConf();
		}
		return instance;
	}
	
	public static String getTime() {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
		return format.format(new Date());
	}
	
	
	public static boolean inSameSubNet(String ipa, String ipb) {
		String[] valuesA = ipa.split(".");
		String[] valuesB = ipb.split(".");
		if (valuesA.length == valuesB.length) {
			boolean result = true;
			for(int i = 0; i < valuesA.length - 1; i++) {
				if (!valuesA[i].equals(valuesB[i])) {
					result = false;
					break;
				}
			}
			return result;
		}
		return false;
	}
	
	public String masterIP;
	public String localIP;
	
	public String indexServerIP;
	public Boolean isIndexServer;
	
	//for zooKeeper
	public String zoo_basePath_gSchema;
	public String zoo_url;
	public String zoo_basePath_dSchema;
	public String zoo_basePath;
	
	//for ServiceThread
	public Thread serviceThread;
	
	public boolean isMaster;
	
	
	
	//Static Variables
	public int gServer_graph_rBuffer_edge_size;
	public int gServer_graph_rBuffer_vertex_size;
	public int gServer_graph_wBuffer_edge_size;
	public int gServer_graph_wBuffer_vertex_size;
	public int gServer_graph_buffer_schema_size;
	public int gServer_graph_index_global_size;
	public int gServer_graph_index_local_size;
	public int gServer_data_pathIndex_global_size;
	public int gServer_data_buffer_index_size;
	public int gServer_data_buffer_schema_size;
	public String gServer_data_index_setup_jarPath;
	public int gServer_usage_cpu;
	public int gServer_usage_mem;
	
	//for HDFS
	public String hdfs_basePath;
	public String hdfs_basePath_data_index;
	public String hdfs_tempPath_data_index;
	
	//for RPC
	public int RPC_GSERVER_PORT = 8889;
	public int RPC_GMASTER_PORT = 8888;
	public int RPC_GCLIENT_PORT = 8890;
	public static int RPC_VERSION = 0;
	
}
