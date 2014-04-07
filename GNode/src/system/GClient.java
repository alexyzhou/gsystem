package system;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import rpc.GMasterProtocol;
import rpc.GServerProtocol;
import data.io.EdgeInfo;
import data.io.VertexInfo;
import data.io.VertexInfo._EdgeInfo;
import data.writable.EdgeCollectionWritable;
import ds.bplusTree.BPlusTree;

public class GClient {

	static int VERTEX_NUM = 50;
	static int MAX_EDGE_NUM_PER_VERTEX = 10;
	protected static final String MASTER_IP = "server.master.ip";

	protected static int[] getRandomValues(int num) {
		int[] intRet = new int[num];
		int intRd = 0; // 存放随机数
		int count = 0; // 记录生成的随机数个数
		int flag = 0; // 是否已经生成过标志
		while (count < num) {
			Random rdm = new Random(System.currentTimeMillis());
			intRd = Math.abs(rdm.nextInt()) % VERTEX_NUM + 1;
			for (int i = 0; i < count; i++) {
				if (intRet[i] == intRd) {
					flag = 1;
					break;
				} else {
					flag = 0;
				}
			}
			if (flag == 0) {
				intRet[count] = intRd;
				count++;
			}
		}
		return intRet;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		BPlusTree<String, String> vLocationIndexTree = new BPlusTree<String, String>(100);

		System.out.println("[Client]" + SystemConf.getTime()
				+ "Generating Data...");

		Random rdm = new Random(System.currentTimeMillis());

		LinkedList<VertexInfo> vertexData = new LinkedList<VertexInfo>();
		HashMap<String, LinkedList<EdgeInfo>> edgeData = new HashMap<String, LinkedList<EdgeInfo>>();

		for (int i = 0; i < VERTEX_NUM; i++) {
			VertexInfo info = new VertexInfo();
			String vid = "v" + new Integer(i).toString();
			info.setId(vid);
			info.setSchema_id("s1");
			info.setGraph_id("graph1");
			//info.setPointer_List(null);
			
			LinkedList<EdgeInfo> edges = new LinkedList<EdgeInfo>();

			int edgeSize = Math.abs(rdm.nextInt()) % 5 + 1;
			//System.out.println(edgeSize);
			int[] randomvalues = getRandomValues(edgeSize);
			if (randomvalues != null) {
				for (int j = 0; j < edgeSize; j++) {
					EdgeInfo tmpEdge = new EdgeInfo();
					String eid = "e" + new Integer(edgeData.size());
					tmpEdge.setId(eid);
					//tmpEdge.setPointer_List(null);
					tmpEdge.setSchema_id("es1");
					tmpEdge.setSource_vertex_id("v" + new Integer(i).toString());
					tmpEdge.setTarget_vertex_id("v"
							+ new Integer(randomvalues[j]).toString());
					edges.add(tmpEdge);
					_EdgeInfo tp = new _EdgeInfo(tmpEdge.getId(),
							tmpEdge.getTarget_vertex_id());
					info.getEdge_List().add(tp);
				}
			}
			vertexData.add(info);
			edgeData.put(vid, edges);
		}

		System.out.println("[Client]" + SystemConf.getTime()
				+ "Generating Data FINISHED!");

		System.out.println("[Client]" + SystemConf.getTime()
				+ "Trying to Send Data...");
		String beginTime = SystemConf.getTime();
		
		Properties prop = new Properties();
		
		HashMap<String, GServerProtocol> gSProtocols = new HashMap<String, GServerProtocol>();
		
		try {
			InputStream in = new FileInputStream("/home/alex/GS/systemconf.properties");
			prop.load(in);
			
			String masterIP = prop.getProperty(MASTER_IP);
			InetSocketAddress address = new InetSocketAddress(masterIP,SystemConf.getInstance().RPC_GMASTER_PORT);
			GMasterProtocol gMprotocol = (GMasterProtocol) RPC.waitForProxy(GMasterProtocol.class, SystemConf.RPC_VERSION, address, new Configuration());
			
			
			for (VertexInfo v : vertexData) {
				String resultIP = gMprotocol.findTargetGServer_Store(v);
				//System.out.println("resultIP" + resultIP);
				if (resultIP != "") {
					if (gSProtocols.get(resultIP) == null) {
						address = new InetSocketAddress(resultIP,SystemConf.getInstance().RPC_GSERVER_PORT);
						GServerProtocol gSProtocol = (GServerProtocol) RPC.waitForProxy(GServerProtocol.class, SystemConf.RPC_VERSION, address, new Configuration());
						gSProtocols.put(resultIP, gSProtocol);
					}
					gSProtocols.get(resultIP).storeVertex(v, new EdgeCollectionWritable(edgeData.get(v.getId())));
					vLocationIndexTree.insertOrUpdate(v.getId(), resultIP);
				} else {
					System.err.println("[Client]" + SystemConf.getTime()
							+ "[ERROR] Vertex " + v.getId() + " findgServer failed");
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("[Client] Data Storage BeginTime" + beginTime);
		System.out.println("[Client]" + SystemConf.getTime()
				+ "Data Storage FINISHED!");
		
		System.out.println("[Client]" + SystemConf.getTime()
				+ "Trying to Query Data...");
		
		String vertexIDtoQuery = "v1525";
		String targetIP = vLocationIndexTree.get(vertexIDtoQuery);
		if (targetIP != null) {
			System.out.println("[Client]Query TargetIP"+targetIP);
			if (gSProtocols.get(targetIP) == null) {
				InetSocketAddress address = new InetSocketAddress(targetIP,SystemConf.getInstance().RPC_GSERVER_PORT);
				GServerProtocol gSProtocol;
				try {
					gSProtocol = (GServerProtocol) RPC.waitForProxy(GServerProtocol.class, SystemConf.RPC_VERSION, address, new Configuration());
					gSProtocols.put(targetIP, gSProtocol);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			VertexInfo info = gSProtocols.get(targetIP).getVertexInfo(vertexIDtoQuery);
			if (info != null) {
				System.out.println("[Client]Query Succeed!");
				System.out.println("[Client]Query ToVertex Localtion:");
				for (_EdgeInfo ei : info.getEdge_List()) {
					System.out.println(vLocationIndexTree.get(ei.target_vertex_id));
				}
			} else {
				System.out.println("[Client]Query Failed!");
			}
		}

		System.out.println("[Client]" + SystemConf.getTime()
				+ "Data Query FINISHED!");
		
		
		System.out.println("[Client]" + SystemConf.getTime()
				+ "Trying to Query Data Remote[223]...");
		beginTime = SystemConf.getTime();
		GServerProtocol protocol;
		VertexInfo info;
		for (int i = 0; i < VERTEX_NUM; i++) {
			vertexIDtoQuery = "v"+(Math.abs(rdm.nextInt()) % VERTEX_NUM);
			targetIP = vLocationIndexTree.get(vertexIDtoQuery);
			protocol = gSProtocols.get(targetIP);
			info = protocol.getVertexInfo(vertexIDtoQuery);
			if (info != null) {
				System.out.println("[Client]Query Succeed!");
			} else {
				System.out.println("[Client]Query Failed!");
			}
		}

		System.out.println("[Client] Data Query BeginTime:"+beginTime);
		System.out.println("[Client]" + SystemConf.getTime()
				+ "Data Query FINISHED!");
		
		System.out.println("[Client]" + SystemConf.getTime()
				+ "Trying to Delete Data Remote[223]...");
		
		String vertexIDtoDelete = "v101";
		protocol = gSProtocols.get("10.60.0.223");
		System.out.println("[Client] Data Deletion " + protocol.removeVertex(vertexIDtoDelete) + "Successfully");
		
		System.out.println("[Client]" + SystemConf.getTime()
				+ "Data Deletion FINISHED!");
		
	}

}
