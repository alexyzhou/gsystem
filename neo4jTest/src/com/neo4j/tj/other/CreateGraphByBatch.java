package com.neo4j.tj.other;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class CreateGraphByBatch {

	//private static final String SERVER_ROOT_URI = "http://192.168.204.128:7474/db/data/";
	private static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";

	private static final String ATTR_NODE_ID = "ID";
	private static final String ATTR_NODE_FLAG = "Restriction_Flag";
	private static final String ATTR_NODE_CREATION_TIME = "Creation_Time";
	private static final String ATTR_NODE_EMAILDOMAIN = "Email_Domain";

	private static final String ATTR_EDGE_ID = "ID";
	private static final String ATTR_EDGE_TRANSACTION_TIME = "Transaction_Time";
	private static final String ATTR_EDGE_SENDER_IP = "Sender_IP";
	private static final String ATTR_EDGE_RECEIVER_IP = "Receiver_IP";
	private static final String ATTR_EDGE_PAYMENT_AMOUNT = "Payment_Amount";
	private static final String ATTR_EDGE_CC_FLAG = "CC_Flag";
	private static final String ATTR_EDGE_CC_ID = "CC_ID";
	private static final String ATTR_EDGE_FRAUD = "Fraud_Flag";

	private static final String ATTR_EDGE_TYPE_TRAN = "Transfer_Money";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		checkDatabaseIsRunning();

		CreateGraphByBatch objBatch = new CreateGraphByBatch();
		objBatch.createSimpleGraphFromFile("/Users/alex/Documents/exData/20000.csv");
	}

	private Map<String, String> nodeIDs = new HashMap<String, String>();
	private Long taskID;

	public void createSimpleGraphFromFile(String filePath) {

		BufferedReader bReader;
		try {
			bReader = new BufferedReader(new FileReader(new File(filePath)));

			String line = null;

			// initialization
			nodeIDs.clear();
			taskID = 0l;

			StringBuffer sbu = new StringBuffer();
			sbu.append("[");
			
			int i = 0;

			while ((line = bReader.readLine()) != null && (i++) != 2000) {
				
				if (i < 1000) continue;
				
				String[] values = line.split(",");
				// sender
				sbu.append(appendStringByCheckAndInsertAccount(values[1],
						values[2], values[3], values[4]));
				// receiver
				sbu.append(appendStringByCheckAndInsertAccount(values[5],
						values[6], values[7], values[8]));
				// transaction
				sbu.append(appendStringByInsertTransaction(nodeIDs.get(values[1]),
						nodeIDs.get(values[5]), values[0], values[9], values[10],
						values[11], values[12], values[13], values[14],
						values[15]));
			}
			
			sbu.deleteCharAt(sbu.length()-1);
			sbu.append("]");
			
			long beginTime = new Date().getTime();
			
			sendCmd(sbu.toString());
			
			System.err.println(new Date().getTime() - beginTime);

			bReader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String appendStringByCheckAndInsertAccount(String... attributes) {
		if (nodeIDs.get(attributes[0]) != null) {
			// already inserted this node
			return "";
		} else {
			Map<String, String> attris = new HashMap<>();
			attris.put(ATTR_NODE_ID, attributes[0]);
			attris.put(ATTR_NODE_FLAG, attributes[1]);
			attris.put(ATTR_NODE_CREATION_TIME, attributes[2]);
			attris.put(ATTR_NODE_EMAILDOMAIN, attributes[3]);

			nodeIDs.put(attributes[0], (++taskID).toString());
			
			return appendStringByInsertNode(taskID.toString(), attris) + ",";
		}
	}

	private String appendStringByInsertTransaction(String sourceID,
			String desID, String... attributes) {
		Map<String, String> attris = new HashMap<>();

		attris.put(ATTR_EDGE_ID, attributes[0]);
		attris.put(ATTR_EDGE_TRANSACTION_TIME, attributes[1]);
		attris.put(ATTR_EDGE_SENDER_IP, attributes[2]);
		attris.put(ATTR_EDGE_RECEIVER_IP, attributes[3]);
		attris.put(ATTR_EDGE_PAYMENT_AMOUNT, attributes[4]);
		attris.put(ATTR_EDGE_CC_FLAG, attributes[5]);
		attris.put(ATTR_EDGE_CC_ID, attributes[6]);
		attris.put(ATTR_EDGE_FRAUD, attributes[7]);

		return appendStringByInsertEdge((++taskID).toString(), sourceID, desID,
				ATTR_EDGE_TYPE_TRAN, attris) + ",";
	}

	protected static void createSimpleGraphTest() {

		StringBuilder cmdBuilder = new StringBuilder();

		cmdBuilder.append("[");

		Map<String, String> attris = new HashMap<>();
		// node 0
		attris.put("name", "bob");
		cmdBuilder.append(appendStringByInsertNode("403", attris));
		cmdBuilder.append(",");
		// node 1
		attris.clear();
		attris.put("age", "12");
		cmdBuilder.append(appendStringByInsertNode("1", attris));
		cmdBuilder.append(",");
		// edge 0
		attris.clear();
		attris.put("since", "2010");
		cmdBuilder.append(appendStringByInsertEdge("2", "403", "1", "KNOWS",
				attris));
		cmdBuilder.append(",");
		// edge 0 With Index
		cmdBuilder.append(appendStringByInsertEdgeIndex("3", "2", "since",
				"2010"));
		cmdBuilder.append("]");
		sendCmd(cmdBuilder.toString());
	}

	protected static void sendCmd(String cmd) {
		// START SNIPPET: createNode
		final String nodeEntryPointUri = SERVER_ROOT_URI + "batch";
		// http://localhost:7474/db/data/node

		WebResource resource = Client.create().resource(nodeEntryPointUri);
		// POST JSON to the relationships URI
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON).entity(cmd)
				.post(ClientResponse.class);
		
		System.out.println(String.format("POST to [%s], status code [%d]",
				nodeEntryPointUri, response.getStatus()));

		response.close();

		return;
		// END SNIPPET: createNode
	}

	protected static String appendStringByInsertNode(String id,
			Map<String, String> node) {
		// {
		// "method" : "POST",
		// "to" : "/node",
		// "id" : 0,
		// "body" : {
		// "name" : "bob"
		// }
		// }

		StringBuilder sb = new StringBuilder();
		sb.append("{ \"method\" : \"POST\",");
		sb.append(" \"to\" : \"/node\",");
		sb.append(" \"id\" : " + id);

		if (node.isEmpty()) {
			sb.append("}");
		} else {
			sb.append(", \"body\" : {");

			for (Map.Entry<String, String> entry : node.entrySet()) {
				sb.append("\"" + entry.getKey() + "\" : \"" + entry.getValue()
						+ "\",");
			}
			// remove the last ","
			sb.deleteCharAt(sb.length() - 1);

			sb.append("}}");
		}

		return sb.toString();
	}

	protected static String appendStringByInsertEdge(String id,
			String sourceNodeID, String desNodeID, String edgeType,
			Map<String, String> edge) {
		// {
		// "method" : "POST",
		// "to" : "{0}/relationships",
		// "id" : 3,
		// "body" : {
		// "to" : "{1}",
		// "data" : {
		// "since" : "2010"
		// },
		// "type" : "KNOWS"
		// }
		// }

		StringBuilder sb = new StringBuilder();
		sb.append("{ \"method\" : \"POST\",");
		sb.append(" \"to\" : \"{" + sourceNodeID + "}/relationships\",");
		sb.append(" \"id\" : " + id + ",");
		sb.append(" \"body\" : { \"to\" : \"{" + desNodeID + "}\",");

		if (edge.isEmpty()) {

		} else {
			sb.append(" \"data\" : {");
			for (Map.Entry<String, String> entry : edge.entrySet()) {
				sb.append("\"" + entry.getKey() + "\" : \"" + entry.getValue()
						+ "\",");
			}
			// remove the last ","
			sb.deleteCharAt(sb.length() - 1);
			sb.append("},");
		}

		sb.append(" \"type\" : \"" + edgeType + "\" }}");

		return sb.toString();
	}

	protected static String appendStringByInsertEdgeIndex(String id,
			String edgeId, String key, String value) {
		// {
		// "method" : "POST",
		// "to" : "/index/relationship/my_rels",
		// "id" : 4,
		// "body" : {
		// "key" : "since",
		// "value" : "2010",
		// "uri" : "{3}"
		// }
		// }

		StringBuilder sb = new StringBuilder();
		sb.append("{ \"method\" : \"POST\",");
		sb.append(" \"to\" : \"/index/relationship/my_rels\",");
		sb.append(" \"id\" : " + id + ",");
		sb.append(" \"body\" : { \"key\" : \"" + key + "\", \"value\" : \""
				+ value + "\", \"uri\" : \"{" + edgeId + "}\"}}");

		return sb.toString();
	}

	protected static void checkDatabaseIsRunning() {
		// START SNIPPET: checkServer
		WebResource resource = Client.create().resource(SERVER_ROOT_URI);
		ClientResponse response = resource.get(ClientResponse.class);

		System.out.println(String.format("GET on [%s], status code [%d]",
				SERVER_ROOT_URI, response.getStatus()));
		response.close();
		// END SNIPPET: checkServer
	}

}
