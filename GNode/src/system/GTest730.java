package system;

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
import data.io.DS_DataType;
import data.io.DataPointers_Entity;
import data.io.DataPointers_Entity._DSInfo;
import data.io.Data_Schema;
import data.io.EdgeInfo;
import data.io.Graph_Schema;
import data.io.Graph_Schema.Attribute;
import data.io.VertexData;
import data.io.VertexInfo;
import data.writable.EdgeCollectionWritable;

public class GTest730 {

	private static String GRAPH_DESCRIPTION_FILEPATH = "/home/alex/Documents/DATA/part-r-00000";
	private static int VERTEX_NUM = 50;
	
	public static void main(String[] args) {

		GServerProtocol gsProtocol;
		GMasterProtocol mProtocol;

		// Insert DataSet

		System.out.println("Now try to insert dataset!");
		try {
			gsProtocol = RpcIOCommons.getGServerProtocol("10.60.0.222");
			gsProtocol.insertDataSet("out", "/TestData/out.csv");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("insert dataset failed!");
			return;
		}
		System.out.println("insert dataset succeed!");

		// Insert DataSchema

		System.out.println("Now try to insert dataSchema!");
		Data_Schema schema = new Data_Schema();
		schema.setId("DS0001");
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
			gsProtocol = RpcIOCommons.getGServerProtocol("10.60.0.222");
			gsProtocol.insertOrUpdateDataSchema(schema.getId(), schema);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("insert dataSchema failed!");
			return;
		}
		System.out.println("insert dataSchema succeed!");

		// Insert GraphSchema

		System.out.println("Now try to insert graphSchema!");
		Graph_Schema vertexSchema = new Graph_Schema();
		vertexSchema.setsId("GSV0001");
		ArrayList<Attribute> vertexAttributes = new ArrayList<Attribute>();
		vertexAttributes.add(vertexSchema.new Attribute("id",
				DS_DataType.integer));
		vertexAttributes.add(vertexSchema.new Attribute("restricted",
				DS_DataType.bool));
		vertexAttributes.add(vertexSchema.new Attribute("creation_time",
				DS_DataType.integer));
		vertexAttributes.add(vertexSchema.new Attribute("email_domain",
				DS_DataType.string));
		vertexSchema.setAttributes(vertexAttributes);

		Graph_Schema edgeSchema = new Graph_Schema();
		edgeSchema.setsId("GSE0001");
		ArrayList<Attribute> edgeAttributes = new ArrayList<Attribute>();
		edgeAttributes.add(edgeSchema.new Attribute("id", DS_DataType.integer));
		edgeAttributes.add(edgeSchema.new Attribute("toVertex",
				DS_DataType.integer));
		edgeAttributes.add(edgeSchema.new Attribute("creation_time",
				DS_DataType.integer));
		edgeAttributes.add(edgeSchema.new Attribute("sender_ip",
				DS_DataType.integer));
		edgeAttributes.add(edgeSchema.new Attribute("receiver_ip",
				DS_DataType.integer));
		edgeAttributes.add(edgeSchema.new Attribute("tran_amount",
				DS_DataType.floats));
		edgeAttributes.add(edgeSchema.new Attribute("fraud_flag",
				DS_DataType.bool));
		edgeAttributes.add(edgeSchema.new Attribute("creditCard_id",
				DS_DataType.integer));
		edgeSchema.setAttributes(edgeAttributes);

		try {
			gsProtocol = RpcIOCommons.getGServerProtocol("10.60.0.222");
			gsProtocol.insertOrUpdateSchema("Graph0001", vertexSchema);
			gsProtocol.insertOrUpdateSchema("Graph0001", edgeSchema);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("insert graphSchema failed!");
			return;
		}
		System.out.println("insert graphSchema succeed!");

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

		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(GRAPH_DESCRIPTION_FILEPATH)));
			String line = null;
			int count = 0;
			while ((line = reader.readLine()) != null) {
				if (count++ == VERTEX_NUM) break;
				String[] val1 = line.split("\t");
				String[] eles = val1[0].split("-");

				// First Vertex
				VertexInfo vf = new VertexInfo();
				vf.setId(eles[1]);
				vf.setSchema_id("GSV0001");
				vf.setGraph_id("Graph0001");
				if (!vs.contains(vf)) {
					DataPointers_Entity dp = new DataPointers_Entity();
					for (Attribute attr : vertexSchema.getAttributes()) {
						_DSInfo dsi = new _DSInfo("out", "DS0001", new Long(
								val1[1]), vertexLink1.get(attr.name));
						dp.data.put(attr.name, dsi);
					}
					vf.setPointer_List(dp);
					vs.add(vf);
				}

				// Second Vertex
				VertexInfo vse = new VertexInfo();
				vse.setId(eles[2]);
				vse.setSchema_id("GSV0001");
				vse.setGraph_id("Graph0001");
				if (!vs.contains(vse)) {
					DataPointers_Entity dp = new DataPointers_Entity();
					for (Attribute attr : vertexSchema.getAttributes()) {
						_DSInfo dsi = new _DSInfo("out", "DS0001", new Long(
								val1[1]), vertexLink2.get(attr.name));
						dp.data.put(attr.name, dsi);
					}
					vse.setPointer_List(dp);
					vs.add(vse);
				}

				// Edge
				EdgeInfo ei = new EdgeInfo();
				ei.setId(eles[0]);
				ei.setSchema_id("GSE0001");
				ei.setSource_vertex_id(eles[1]);
				ei.setTarget_vertex_id(eles[2]);
				if (!es.contains(ei)) {
					DataPointers_Entity dp = new DataPointers_Entity();
					for (Attribute attr : edgeSchema.getAttributes()) {
						_DSInfo dsi = new _DSInfo("out", "DS0001", new Long(
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
			return;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("prepare Graph failed!");
			return;
		}

		System.out.println("prepare Graph succeed!");

		// Insert Graph

		System.out.println("Now try to insert Graph!");

		SystemConf.getInstance().masterIP = "10.60.0.221";
		
		try {
			for (VertexInfo v : vs) {
				mProtocol = RpcIOCommons.getMasterProxy();
				String resultIP = mProtocol.findTargetGServer_Store(v);
				// System.out.println("resultIP" + resultIP);
				if (resultIP != "") {
					gsProtocol = RpcIOCommons.getGServerProtocol(resultIP);
					gsProtocol.storeVertex(v, new EdgeCollectionWritable());
				} else {
					System.err.println("[Client]" + SystemConf.getTime()
							+ "[ERROR] Vertex " + v.getId()
							+ " findgServer failed");
					System.out.println("insert Graph failed!");
					return;
				}
			}
			for (EdgeInfo e : es) {
				mProtocol = RpcIOCommons.getMasterProxy();
				String resultIP = mProtocol.findTargetGServer_StoreEdge(e);
				// System.out.println("resultIP" + resultIP);
				if (resultIP != "") {
					gsProtocol = RpcIOCommons.getGServerProtocol(resultIP);
					gsProtocol.storeEdge(e);
				} else {
					System.err.println("[Client]" + SystemConf.getTime()
							+ "[ERROR] Vertex " + e.getId()
							+ " findgServer failed");
					System.out.println("insert Graph failed!");
					return;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("insert Graph failed!");
			return;
		}

		System.out.println("insert Graph succeed!");

		// Query Graph
		
		System.out.println("Now try to query Graph!");
		
		String vertexIDToQuery = "148926";
		String edgeIDToQuery = "1000179";
		
		try {
			gsProtocol = RpcIOCommons.getGServerProtocol("10.60.0.223");
			VertexInfo info = gsProtocol.getVertexInfo_Remote(vertexIDToQuery);
			System.err.println(info.getSchema_id());
			VertexData data = gsProtocol.getVertexData(vertexIDToQuery);
			System.err.println(data.getData().get("creation_time"));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("query Graph succeed!");
	}

}
