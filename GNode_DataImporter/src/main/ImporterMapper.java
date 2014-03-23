package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import rpc.GMasterProtocol;
import rpc.GServerProtocol;
import rpc.RpcIOCommons;
import system.SystemConf;
import system.error.ErrorCode;
import data.io.DS_DataType;
import data.io.DataPointers_Entity;
import data.io.DataPointers_Entity._DSInfo;
import data.io.EdgeInfo;
import data.io.Graph_Schema;
import data.io.Graph_Schema.Attribute;
import data.io.VertexInfo;
import data.writable.EdgeCollectionWritable;

public class ImporterMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static String DATASET_ID = "out_senderid_sorted";
	private static String DATASCHEMA_ID = "DS0001";

	protected static HashMap<String, String> vertexLink1 = new HashMap<>();
	protected static HashMap<String, String> vertexLink2 = new HashMap<>();
	protected static HashMap<String, String> edgeLink = new HashMap<>();

	protected static Graph_Schema vertexSchema;
	protected static Graph_Schema edgeSchema;

	private static String GRAPH_ID = "Graph0001";
	private static String VERTEXSCHEMA_ID = "GSV0001";
	private static String EDGESCHEMA_ID = "GSE0001";
	
	private static Text vertexText = new Text("v");
	private static Text edgeText = new Text("e");
	

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);

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

		vertexSchema = get_VertexSchema();
		edgeSchema = get_EdgeSchema();
		
		SystemConf.getInstance().masterIP = context.getConfiguration().get("GNMasterIP");
		DATASET_ID = context.getConfiguration().get("GNDSID");
		
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String[] values = value.toString().split(",");
		if (values.length == 16) {

			// First Vertex
			VertexInfo vf = new VertexInfo();
			vf.setId(values[1]);
			vf.setSchema_id(VERTEXSCHEMA_ID);
			vf.setGraph_id(GRAPH_ID);

			DataPointers_Entity dp = new DataPointers_Entity();
			for (Attribute attr : vertexSchema.getAttributes()) {
				_DSInfo dsi = new _DSInfo(DATASET_ID, DATASCHEMA_ID, key.get(),
						vertexLink1.get(attr.name));
				dp.data.put(attr.name, dsi);
			}
			vf.setPointer_List(dp);

			// Second Vertex
			VertexInfo vse = new VertexInfo();
			vse.setId(values[5]);
			vse.setSchema_id(VERTEXSCHEMA_ID);
			vse.setGraph_id(GRAPH_ID);
			DataPointers_Entity dp2 = new DataPointers_Entity();
			for (Attribute attr : vertexSchema.getAttributes()) {
				_DSInfo dsi = new _DSInfo(DATASET_ID, DATASCHEMA_ID, key.get(),
						vertexLink2.get(attr.name));
				dp2.data.put(attr.name, dsi);
			}
			vse.setPointer_List(dp2);

			// Edge
			EdgeInfo ei = new EdgeInfo();
			ei.setId(values[0]);
			ei.setSchema_id(EDGESCHEMA_ID);
			ei.setSource_vertex_id(values[1]);
			ei.setTarget_vertex_id(values[2]);
			DataPointers_Entity dpe = new DataPointers_Entity();
			for (Attribute attr : edgeSchema.getAttributes()) {
				_DSInfo dsi = new _DSInfo(DATASET_ID, DATASCHEMA_ID, key.get(), edgeLink.get(attr.name));
				dpe.data.put(attr.name, dsi);
			}
			ei.setPointer_List(dpe);

			try {
				context.write(vertexText, new Text(storeVertexInfo(vf)));
				context.write(vertexText, new Text(storeVertexInfo(vse)));
				context.write(edgeText, new Text(storeEdgeInfo(ei)));
			} catch (IOException e) {
				System.err.println(e.getLocalizedMessage());
			}

		}
		
		
	}
	
	protected String storeVertexInfo(VertexInfo v) throws IOException {
		GMasterProtocol mProtocol = RpcIOCommons.getMasterProxy();
		
		String resultIP = mProtocol.findTargetGServer_Store(v);
		if (!(resultIP.equals(ErrorCode.VERTEX_ALREADYEXIST) || resultIP.equals(""))) {
			GServerProtocol gsProtocol = RpcIOCommons
					.getGServerProtocol(resultIP);
			gsProtocol.storeVertex(v, new EdgeCollectionWritable());
			return "Succeed With "+v.getId();
		} else {
			System.err.println("[Client]" + SystemConf.getTime()
					+ "[ERROR] Vertex " + v.getId()
					+ " findgServer failed");
			System.err.println("insert Graph failed!");
			return "";
		}
		
	}
	
	protected String storeEdgeInfo(EdgeInfo e) throws IOException {
		
		GMasterProtocol mProtocol = RpcIOCommons.getMasterProxy();
		String resultIP = mProtocol.findTargetGServer_StoreEdge(e);
		
		if (!(resultIP.equals(ErrorCode.EDGE_ALREADYEXIST) || resultIP.equals(""))) {
			GServerProtocol gsProtocol = RpcIOCommons
					.getGServerProtocol(resultIP);
			gsProtocol.storeEdge(e);
			return "Succeed E Wtih "+e.getId();
		} else {
			System.err.println("[Client]" + SystemConf.getTime()
					+ "[ERROR] Vertex " + e.getId()
					+ " findgServer failed");
			System.err.println("insert Graph failed!");
			return "";
		}
	}

	protected static Graph_Schema get_VertexSchema() {
		Graph_Schema vertexSchema = new Graph_Schema();
		vertexSchema.setsId(VERTEXSCHEMA_ID);
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
		return vertexSchema;
	}

	protected static Graph_Schema get_EdgeSchema() {
		Graph_Schema edgeSchema = new Graph_Schema();
		edgeSchema.setsId(EDGESCHEMA_ID);
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
		return edgeSchema;
	}

}
