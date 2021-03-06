package main;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.Mapper;

import rpc.GMasterProtocol;
import rpc.GServerProtocol;
import rpc.RpcIOCommons;
import system.SystemConf;
import system.error.ErrorCode;
import test.Debug;
import test.TestVariables;
import data.io.DataPointers_Entity;
import data.io.DataPointers_Entity._DSInfo;
import data.io.EdgeInfo;
import data.io.Graph_Schema;
import data.io.Graph_Schema.Attribute;
import data.io.VertexInfo;
import data.writable.EdgeCollectionWritable;
import data.writable.VertexCollectionWritable;

public class ImporterMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	

	protected static HashMap<String, String> vertexLink1 = new HashMap<>();
	protected static HashMap<String, String> vertexLink2 = new HashMap<>();
	protected static HashMap<String, String> edgeLink = new HashMap<>();

	protected static Graph_Schema vertexSchema;
	protected static Graph_Schema edgeSchema;
	
	protected static String dataSetID = "";
	
	protected LinkedList<VertexInfo> vertexSet;
	protected LinkedList<EdgeInfo> edgeSet;
	
	
//	private static Text vertexText = new Text("v");
//	private static Text edgeText = new Text("e");
	

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

		vertexSchema = TestVariables.get_VertexSchema();
		edgeSchema = TestVariables.get_EdgeSchema();
		
		SystemConf.getInstance().masterIP = TestVariables.MASTER_IP;
		dataSetID = context.getConfiguration().get("GNDSID");
		
		if (vertexSet == null) {
			vertexSet = new LinkedList<>();
		} else {
			vertexSet.clear();
		}
		if (edgeSet == null) {
			edgeSet = new LinkedList<>();
		} else {
			edgeSet.clear();
		}
	}
	
	

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		
		VertexCollectionWritable vColl = new VertexCollectionWritable(vertexSet);
		EdgeCollectionWritable eColl = new EdgeCollectionWritable(edgeSet);
		
		try {
			GMasterProtocol proxy = RpcIOCommons.getMasterProxy();
			proxy.storeVertexAndEdgeList(vColl, eColl);
			RpcIOCommons.freeMasterProxy();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		RpcIOCommons.stop();
	}



	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {

		String[] values = value.toString().split(",");
		if (values.length == 16) {

			// First Vertex
			VertexInfo vf = new VertexInfo();
			vf.setId(values[1]);
			vf.setSchema_id(TestVariables.VERTEXSCHEMA_ID);
			vf.setGraph_id(TestVariables.GRAPH_ID);

			DataPointers_Entity dp = new DataPointers_Entity();
			for (Attribute attr : vertexSchema.getAttributes()) {
				_DSInfo dsi = new _DSInfo(dataSetID, TestVariables.DATASCHEMA_ID, key.get(),
						vertexLink1.get(attr.name));
				dp.data.put(attr.name, dsi);
			}
			vf.setPointer_List(dp);

			// Second Vertex
			VertexInfo vse = new VertexInfo();
			vse.setId(values[5]);
			vse.setSchema_id(TestVariables.VERTEXSCHEMA_ID);
			vse.setGraph_id(TestVariables.GRAPH_ID);
			DataPointers_Entity dp2 = new DataPointers_Entity();
			for (Attribute attr : vertexSchema.getAttributes()) {
				_DSInfo dsi = new _DSInfo(dataSetID, TestVariables.DATASCHEMA_ID, key.get(),
						vertexLink2.get(attr.name));
				dp2.data.put(attr.name, dsi);
			}
			vse.setPointer_List(dp2);

			// Edge
			EdgeInfo ei = new EdgeInfo();
			ei.setId(values[0]);
			ei.setSchema_id(TestVariables.EDGESCHEMA_ID);
			ei.setSource_vertex_id(values[1]);
			ei.setTarget_vertex_id(values[5]);
			DataPointers_Entity dpe = new DataPointers_Entity();
			for (Attribute attr : edgeSchema.getAttributes()) {
				_DSInfo dsi = new _DSInfo(dataSetID, TestVariables.DATASCHEMA_ID, key.get(), edgeLink.get(attr.name));
				dpe.data.put(attr.name, dsi);
			}
			ei.setPointer_List(dpe);

			//try {
				
				//storeVertexInfo(vf);
				//storeVertexInfo(vse);
				//storeEdgeInfo(ei);
//				context.write(vertexText, new Text(storeVertexInfo(vf)));
//				context.write(vertexText, new Text(storeVertexInfo(vse)));
//				context.write(edgeText, new Text(storeEdgeInfo(ei)));
			//} catch (IOException e) {
				//System.err.println(e.getLocalizedMessage());
			//}
				
			vertexSet.add(vf);
			vertexSet.add(vse);
			edgeSet.add(ei);
			
		}
		
		
	}
	
	protected void storeVertexInfo(VertexInfo v) throws IOException {
		GMasterProtocol mProtocol = RpcIOCommons.getMasterProxy();
		
		String resultIP = mProtocol.findTargetGServer_Store(v);
		RpcIOCommons.freeMasterProxy();
		if (!(resultIP == null || resultIP.equals(ErrorCode.VERTEX_ALREADYEXIST) || resultIP.equals(""))) {
			GServerProtocol gsProtocol = RpcIOCommons
					.getGServerProtocol(resultIP);
			gsProtocol.storeVertexAndUpdateIndex(v);
			RpcIOCommons.freeGServerProtocol(resultIP);
			//return "Succeed With "+v.getId();
		} else {
			System.err.println("[Client]" + SystemConf.getTime()
					+ "[ERROR] Vertex " + v.getId()
					+ " findgServer failed");
			System.err.println("insert Graph failed!");
			//return "";
		}
		
	}
	
	protected void storeEdgeInfo(EdgeInfo e) throws IOException {
		
		GMasterProtocol mProtocol = RpcIOCommons.getMasterProxy();
		String resultIP = mProtocol.findTargetGServer_StoreEdge(e);
		RpcIOCommons.freeMasterProxy();
		if (!(resultIP == null || resultIP.equals(ErrorCode.EDGE_ALREADYEXIST) || resultIP.equals(""))) {
			GServerProtocol gsProtocol = RpcIOCommons
					.getGServerProtocol(resultIP);
			gsProtocol.storeEdgeAndUpdateVertex(e);
			RpcIOCommons.freeGServerProtocol(resultIP);
			//return "Succeed E Wtih "+e.getId();
		} else {
			System.err.println("[Client]" + SystemConf.getTime()
					+ "[ERROR] Vertex " + e.getId()
					+ " findgServer failed");
			System.err.println("insert Graph failed!");
			//return "";
		}
	}

	

}
