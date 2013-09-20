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
	public String zoo_gnode_base_path;
	public String zoo_url;
	public String zoo_gp_schema_basePath;
	
	//for ServiceThread
	public Thread serviceThread;
	
	public boolean isMaster;
	
	//Static Variables
	public static int GSERVER_MAX_WBUFFER_VERTEX = 50;
	public static int GSERVER_MAX_WBUFFER_EDGE = 50;
	public static int GSERVER_MAX_RBUFFER_VERTEX = 100;
	public static int GSERVER_MAX_RBUFFER_EDGE = 100;
	
	public static int GSERVER_MAX_GP_SCHEMA_CACHE = 10;
	
	public static int GSERVER_LOCALINDEXTREE_FACTOR = 64;
	public static int GSERVER_GLOBALINDEXTREE_FACTOR = 128;
	
	//for Graph Index Factors
	public static double GSERVER_MAXUSAGE_CPU = 90;
	public static double GSERVER_MAXUSAGE_MEM = 80;
	
	//for HDFS
	public String hdfs_basePath = "/GraphDB";
	
	//for RPC
	public int RPC_GSERVER_PORT = 8889;
	public int RPC_GMASTER_PORT = 8888;
	public static int RPC_VERSION = 0;
	
	//for DataLayer
		//for DataSet PathIndex Factors
		public static int DATASET_PATHINDEX_FACTOR = 16;
		//for DataSet Index
		public String hdfs_dsIndex_basePath = "/GraphDB/DataSet/Index";
		public static int DATASET_INDEX_CACHE_FACTOR = 10;
		//for DataSet Schema
		public String zoo_ds_schema_basePath;
		public static int DATASET_SCHEMA_CACHE_FACTOR = 10;
	//End of DataLayer
}
