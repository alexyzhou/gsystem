package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

import rpc.GServerProtocol;
import rpc.RpcIOCommons;
import data.io.DS_DataType;
import data.io.Data_Schema;
import data.io.Graph_Schema;
import data.io.VertexData;
import data.io.VertexInfo;
import data.writable.TraverseJobParameters;
import data.writable.TraverseJobParameters.TraversalMethod;

public class GTest33114 {
	
	protected static boolean insertDataSet_Test(String id, String path) {
		System.out.println("Now try to insert dataset!");

		GServerProtocol gsProtocol;

		try {
			gsProtocol = RpcIOCommons.getGServerProtocol(TestVariables.TARGET_IP);
			gsProtocol.insertDataSet(id, path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("insert dataset failed!");
			return false;
		}
		System.out.println("insert dataset succeed!");
		return true;
	}
	
	protected static boolean insertDataSchema_Test() {
		System.out.println("Now try to insert dataSchema!");
		Data_Schema schema = new Data_Schema();
		schema.setId(TestVariables.DATASCHEMA_ID);
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
					.getGServerProtocol(TestVariables.TARGET_IP);
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

	protected static boolean insertGraphSchema_Test() {
		System.out.println("Now try to insert graphSchema!");
		Graph_Schema vertexSchema = TestVariables.get_VertexSchema();
		Graph_Schema edgeSchema = TestVariables.get_EdgeSchema();

		try {
			GServerProtocol gsProtocol = RpcIOCommons
					.getGServerProtocol(TestVariables.TARGET_IP);
			gsProtocol.insertOrUpdateSchema(TestVariables.GRAPH_ID, vertexSchema);
			gsProtocol.insertOrUpdateSchema(TestVariables.GRAPH_ID, edgeSchema);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("insert graphSchema failed!");
			return false;
		}
		System.out.println("insert graphSchema succeed!");
		return true;
	}
	
	protected static void traverseGraph_Test() {
		// BFS
		System.out.println("Now try to traverse graph!");
		String nodeID = "100001";

		try {
			
			GServerProtocol gsProtocol = RpcIOCommons
					.getGServerProtocol(TestVariables.TARGET_IP);
			TraverseJobParameters param = new TraverseJobParameters(UUID.randomUUID(), TraversalMethod.DFS, 10, (new Date()).getTime());
			
			String targetNode = gsProtocol.queryVertexToServer(nodeID);
			
			if (targetNode != null) {
				if (!targetNode.equals(TestVariables.TARGET_IP)) {
					GServerProtocol gServerPro = RpcIOCommons
							.getGServerProtocol(targetNode);
					gServerPro.traverseGraph_Async(nodeID, param);
				} else {
					gsProtocol.traverseGraph_Async(nodeID, param);
				}
				System.out.println("traverse graph succeed!");
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("traverse graph failed!");
		}

	}
	
	protected static boolean queryGraph_Test() {
		System.out.println("Now try to query Graph!");

		String vertexIDToQuery = "100006";
		String gServerIP = TestVariables.TARGET_IP;

		try {
			
			//For Vertex
			GServerProtocol gsProtocol = RpcIOCommons.getGServerProtocol(gServerIP);
			VertexInfo info = gsProtocol.getVertexInfo(vertexIDToQuery);
			String target = gsProtocol.queryVertexToServer(vertexIDToQuery);
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

	

	public static void main(String[] args) {
		
		//insertDataSet_Test(TestVariables.DATASET_ID, TestVariables.DATASET_PATH);
		
//		insertDataSchema_Test()
//		
//		insertGraphSchema_Test()
		
		//queryGraph_Test();
		traverseGraph_Test();
		
		RpcIOCommons.stop();
		
	}

}
