package test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import rpc.GMasterProtocol;
import rpc.GServerProtocol;
import rpc.RpcIOCommons;
import system.SystemConf;
import data.io.DS_DataType;
import data.io.DataPointers_Entity;
import data.io.Data_Schema;
import data.io.EdgeInfo;
import data.io.Graph_Schema;
import data.io.VertexData;
import data.io.VertexInfo;
import data.io.DataPointers_Entity._DSInfo;
import data.io.Graph_Schema.Attribute;
import data.writable.EdgeCollectionWritable;
import ds.index.BinarySearchStringIndex;

public class GTest926 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		GTest926 test = new GTest926();
		
		SystemConf.getInstance().masterIP = "10.60.0.221";
		
		if (test.insertDataSet_Test()) {
			if (test.insertDataSchema_Test()) {
				if (test.insertDataSetIndex_Test()) {
					//test.queryDataSetIndex_Test();
				}
			}
		}
		
//		test.queryDataSetIndex_Test();
		
		System.exit(0);
	}

	private static String GRAPH_DESCRIPTION_FILEPATH = "/home/alex/Documents/DATA/part-r-00000";
	private static int VERTEX_NUM = 50;
	private static String DATASET_ID = "out_senderid_sorted";
	private static String DATASET_PATH = "/TestData/out.csv";
	private static String DATASCHEMA_ID = "DS0001";
	private static String GRAPH_ID = "Graph0001";
	private static String VERTEXSCHEMA_ID = "GSV0001";
	private static String EDGESCHEMA_ID = "GSE0001";

	protected boolean insertDataSet_Test() {
		System.out.println("Now try to insert dataset!");

		GServerProtocol gsProtocol;

		try {
			gsProtocol = RpcIOCommons.getGServerProtocol("10.60.0.222");
			gsProtocol.insertDataSet(DATASET_ID, DATASET_PATH);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("insert dataset failed!");
			return false;
		}
		System.out.println("insert dataset succeed!");
		return true;
	}

	protected boolean insertDataSchema_Test() {
		System.out.println("Now try to insert dataSchema!");
		Data_Schema schema = new Data_Schema();
		schema.setId(DATASCHEMA_ID);
		schema.setSeperator(',');
		ArrayList<Data_Schema.ColumnDescription> columns = new ArrayList<Data_Schema.ColumnDescription>();
		columns.add(schema.new ColumnDescription("transaction_id",
				DS_DataType.integer, "0", true));
		columns.add(schema.new ColumnDescription("sender_id",
				DS_DataType.integer, "1", true));
		columns.add(schema.new ColumnDescription("sender_restricted_flag",
				DS_DataType.bool, "2", false));
		columns.add(schema.new ColumnDescription(
				"sender_account_creation_time", DS_DataType.integer, "3", true));
		columns.add(schema.new ColumnDescription("sender_email_domain",
				DS_DataType.string, "4", true));
		columns.add(schema.new ColumnDescription("receiver_id",
				DS_DataType.integer, "5", true));
		columns.add(schema.new ColumnDescription("receiver_restricted_flag",
				DS_DataType.bool, "6", false));
		columns.add(schema.new ColumnDescription(
				"receiver_account_creation_time", DS_DataType.integer, "7",
				true));
		columns.add(schema.new ColumnDescription("receiver_email_domain",
				DS_DataType.string, "8", true));
		columns.add(schema.new ColumnDescription("transaction_time",
				DS_DataType.integer, "9", true));
		columns.add(schema.new ColumnDescription("sender_ip",
				DS_DataType.integer, "10", true));
		columns.add(schema.new ColumnDescription("reveiver_ip",
				DS_DataType.integer, "11", true));
		columns.add(schema.new ColumnDescription("tran_amount",
				DS_DataType.integer, "12", true));
		columns.add(schema.new ColumnDescription("fraud_flag",
				DS_DataType.bool, "13", false));
		columns.add(schema.new ColumnDescription("creditcard_id",
				DS_DataType.integer, "14", true));
		columns.add(schema.new ColumnDescription("creditcard_flag",
				DS_DataType.bool, "15", false));
		schema.setColumns(columns);
		try {
			GServerProtocol gsProtocol = RpcIOCommons
					.getGServerProtocol("10.60.0.222");
			gsProtocol.insertOrUpdateDataSchema(schema.getId(), schema);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("insert dataSchema failed!");
			return false;
		}
		System.out.println("insert dataSchema succeed!");
		return true;
	}

	protected boolean insertGraphSchema_Test() {
		System.out.println("Now try to insert graphSchema!");
		Graph_Schema vertexSchema = get_VertexSchema();
		Graph_Schema edgeSchema = get_EdgeSchema();

		try {
			GServerProtocol gsProtocol = RpcIOCommons
					.getGServerProtocol("10.60.0.222");
			gsProtocol.insertOrUpdateSchema(GRAPH_ID, vertexSchema);
			gsProtocol.insertOrUpdateSchema(GRAPH_ID, edgeSchema);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("insert graphSchema failed!");
			return false;
		}
		System.out.println("insert graphSchema succeed!");
		return true;
	}

	protected boolean insertGraph_Test() {

		// Prepare Graph

		System.out.println("Now try to prepare Graph!");
		LinkedList<VertexInfo> vs = new LinkedList<>();
		LinkedList<EdgeInfo> es = new LinkedList<>();

		HashMap<String, String> vertexLink1 = new HashMap<>();
		HashMap<String, String> vertexLink2 = new HashMap<>();
		HashMap<String, String> edgeLink = new HashMap<>();

		vertexLink1.put("id", "sender_id");
		vertexLink1.put("restricted", "sender_restricted_flag");
		vertexLink1.put("creation_time", "sender_account_creation_time");
		vertexLink1.put("email_domain", "sender_email_domain");

		vertexLink2.put("id", "receiver_id");
		vertexLink2.put("restricted", "receiver_restricted_flag");
		vertexLink2.put("creation_time", "receiver_account_creation_time");
		vertexLink2.put("email_domain", "receiver_email_domain");

		edgeLink.put("id", "transaction_id");
		edgeLink.put("toVertex", "receiver_id");
		edgeLink.put("creation_time", "transaction_time");
		edgeLink.put("sender_ip", "sender_ip");
		edgeLink.put("receiver_ip", "reveiver_ip");
		edgeLink.put("tran_amount", "tran_amount");
		edgeLink.put("fraud_flag", "fraud_flag");
		edgeLink.put("creditCard_id", "creditcard_id");

		Graph_Schema vertexSchema = get_VertexSchema();
		Graph_Schema edgeSchema = get_EdgeSchema();

		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(GRAPH_DESCRIPTION_FILEPATH)));
			String line = null;
			int count = 0;
			while ((line = reader.readLine()) != null) {
				if (count++ == VERTEX_NUM)
					break;
				String[] val1 = line.split("\t");
				String[] eles = val1[0].split("-");

				// First Vertex
				VertexInfo vf = new VertexInfo();
				vf.setId(eles[1]);
				vf.setSchema_id(VERTEXSCHEMA_ID);
				vf.setGraph_id(GRAPH_ID);
				if (!vs.contains(vf)) {
					DataPointers_Entity dp = new DataPointers_Entity();
					for (Attribute attr : vertexSchema.getAttributes()) {
						_DSInfo dsi = new _DSInfo(DATASET_ID, DATASCHEMA_ID, new Long(
								val1[1]), vertexLink1.get(attr.name));
						dp.data.put(attr.name, dsi);
					}
					vf.setPointer_List(dp);
					vs.add(vf);
				}

				// Second Vertex
				VertexInfo vse = new VertexInfo();
				vse.setId(eles[2]);
				vse.setSchema_id(VERTEXSCHEMA_ID);
				vse.setGraph_id(GRAPH_ID);
				if (!vs.contains(vse)) {
					DataPointers_Entity dp = new DataPointers_Entity();
					for (Attribute attr : vertexSchema.getAttributes()) {
						_DSInfo dsi = new _DSInfo(DATASET_ID, DATASCHEMA_ID, new Long(
								val1[1]), vertexLink2.get(attr.name));
						dp.data.put(attr.name, dsi);
					}
					vse.setPointer_List(dp);
					vs.add(vse);
				}

				// Edge
				EdgeInfo ei = new EdgeInfo();
				ei.setId(eles[0]);
				ei.setSchema_id(EDGESCHEMA_ID);
				ei.setSource_vertex_id(eles[1]);
				ei.setTarget_vertex_id(eles[2]);
				if (!es.contains(ei)) {
					DataPointers_Entity dp = new DataPointers_Entity();
					for (Attribute attr : edgeSchema.getAttributes()) {
						_DSInfo dsi = new _DSInfo(DATASET_ID, DATASCHEMA_ID, new Long(
								val1[1]), edgeLink.get(attr.name));
						dp.data.put(attr.name, dsi);
					}
					ei.setPointer_List(dp);
					es.add(ei);
				}
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("prepare Graph failed!");
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("prepare Graph failed!");
			return false;
		}

		System.out.println("prepare Graph succeed!");

		// insert Graph

		System.out.println("Now try to insert Graph!");

		try {
			for (VertexInfo v : vs) {
				GMasterProtocol mProtocol = RpcIOCommons.getMasterProxy();
				String resultIP = mProtocol.findTargetGServer_Store(v);
				// System.out.println("resultIP" + resultIP);
				if (resultIP != "") {
					GServerProtocol gsProtocol = RpcIOCommons
							.getGServerProtocol(resultIP);
					gsProtocol.storeVertex(v, new EdgeCollectionWritable());
				} else {
					System.err.println("[Client]" + SystemConf.getTime()
							+ "[ERROR] Vertex " + v.getId()
							+ " findgServer failed");
					System.out.println("insert Graph failed!");
					return false;
				}
			}
			for (EdgeInfo e : es) {
				GMasterProtocol mProtocol = RpcIOCommons.getMasterProxy();
				String resultIP = mProtocol.findTargetGServer_StoreEdge(e);
				// System.out.println("resultIP" + resultIP);
				if (resultIP != "") {
					GServerProtocol gsProtocol = RpcIOCommons
							.getGServerProtocol(resultIP);
					gsProtocol.storeEdge(e);
				} else {
					System.err.println("[Client]" + SystemConf.getTime()
							+ "[ERROR] Vertex " + e.getId()
							+ " findgServer failed");
					System.out.println("insert Graph failed!");
					return false;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("insert Graph failed!");
			return false;
		}

		System.out.println("insert Graph succeed!");
		return true;
	}
	
	protected boolean insertDataSetIndex_Test() {
		String dsID = DATASET_ID;
		String dschemaID = DATASCHEMA_ID;
		String attriName = "sender_id";
		
		try {
			GMasterProtocol protocol = RpcIOCommons.getMasterProxy();
			if (protocol.createDSIndex(dsID, dschemaID, attriName).equals("")) {
				return true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return false;
	}
	
	protected boolean queryDataSetIndex_Test() {
		GServerProtocol protocol;
		String dsID = DATASET_ID;
		String dschemaID = DATASCHEMA_ID;
		String attriName = "sender_id";
		try {
			protocol = RpcIOCommons.getGServerProtocol("10.60.0.222");
			BinarySearchStringIndex bsi = protocol.getDSIndex(dsID, dschemaID, attriName);
			if (bsi != null) {
				System.err.println(bsi.getOffsets().get(2).get(2));
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
 
	protected boolean queryGraph_Test() {
		System.out.println("Now try to query Graph!");

		String vertexIDToQuery = "148926";
		String edgeIDToQuery = "1000179";
		String gServerIP = "10.60.0.223";

		try {
			
			//For Vertex
			GServerProtocol gsProtocol = RpcIOCommons.getGServerProtocol(gServerIP);
			VertexInfo info = gsProtocol.getVertexInfo_Remote(vertexIDToQuery);
			System.err.println(info.getSchema_id());
			VertexData data = gsProtocol.getVertexData(vertexIDToQuery);
			System.err.println(data.getData().get("creation_time"));
			
			//For Edge
			//TODO
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

		System.out.println("query Graph succeed!");
		return true;
	}

	protected Graph_Schema get_VertexSchema() {
		Graph_Schema vertexSchema = new Graph_Schema();
		vertexSchema.setsId(VERTEXSCHEMA_ID);
		ArrayList<Attribute> vertexAttributes = new ArrayList<Attribute>();
		vertexAttributes.add(vertexSchema.new Attribute("id",
				DS_DataType.integer, DATASET_ID));
		vertexAttributes.add(vertexSchema.new Attribute("restricted",
				DS_DataType.bool, DATASET_ID));
		vertexAttributes.add(vertexSchema.new Attribute("creation_time",
				DS_DataType.integer, DATASET_ID));
		vertexAttributes.add(vertexSchema.new Attribute("email_domain",
				DS_DataType.string, DATASET_ID));
		vertexSchema.setAttributes(vertexAttributes);
		return vertexSchema;
	}

	protected Graph_Schema get_EdgeSchema() {
		Graph_Schema edgeSchema = new Graph_Schema();
		edgeSchema.setsId(EDGESCHEMA_ID);
		ArrayList<Attribute> edgeAttributes = new ArrayList<Attribute>();
		edgeAttributes.add(edgeSchema.new Attribute("id", DS_DataType.integer,
				DATASET_ID));
		edgeAttributes.add(edgeSchema.new Attribute("toVertex",
				DS_DataType.integer, DATASET_ID));
		edgeAttributes.add(edgeSchema.new Attribute("creation_time",
				DS_DataType.integer, DATASET_ID));
		edgeAttributes.add(edgeSchema.new Attribute("sender_ip",
				DS_DataType.integer, DATASET_ID));
		edgeAttributes.add(edgeSchema.new Attribute("receiver_ip",
				DS_DataType.integer, DATASET_ID));
		edgeAttributes.add(edgeSchema.new Attribute("tran_amount",
				DS_DataType.floats, DATASET_ID));
		edgeAttributes.add(edgeSchema.new Attribute("fraud_flag",
				DS_DataType.bool, DATASET_ID));
		edgeAttributes.add(edgeSchema.new Attribute("creditCard_id",
				DS_DataType.integer, DATASET_ID));
		edgeSchema.setAttributes(edgeAttributes);
		return edgeSchema;
	}

}
