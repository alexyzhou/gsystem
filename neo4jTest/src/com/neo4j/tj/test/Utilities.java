package com.neo4j.tj.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import com.neo4j.tj.other.TraversalDefinition;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class Utilities {

	private static String SERVER_ROOT_URI = "http://127.0.0.1:7474/db/data/";

	private static final int BATCH_MAX_TRANS = 10000;

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

	private static Map<String, String> nodeIDs = new HashMap<String, String>();
	private static Long taskID;

	protected static void checkDatabaseIsRunning() {
		// START SNIPPET: checkServer
		WebResource resource = Client.create().resource(SERVER_ROOT_URI);
		ClientResponse response = resource.get(ClientResponse.class);

		System.out.println(String.format("GET on [%s], status code [%d]",
				SERVER_ROOT_URI, response.getStatus()));
		response.close();
		// END SNIPPET: checkServer
	}

	protected static void sendCmd(String cmd) {
		sendCmd(cmd, false);
	}

	protected static void sendCmd(String cmd, boolean outputLog) {
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

		if (outputLog) {
			try {
				BufferedWriter bw = new BufferedWriter(new FileWriter(new File(
						"id.txt")));
				if (bw != null) {
					bw.write(response.getEntity(String.class));
				}
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

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

	private static String appendStringByCheckAndInsertAccount(
			String... attributes) {
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
	
	private static String createNodeByCheckAndInsertAccount(String... attributes) {
		if (nodeIDs.get(attributes[0]) != null) {
			// already inserted this node
			return nodeIDs.get(attributes[0]);
		} else {
			Map<String, String> attris = new HashMap<>();
			attris.put(ATTR_NODE_ID, attributes[0]);
			attris.put(ATTR_NODE_FLAG, attributes[1]);
			attris.put(ATTR_NODE_CREATION_TIME, attributes[2]);
			attris.put(ATTR_NODE_EMAILDOMAIN, attributes[3]);

			// START SNIPPET: createNode
	        final String nodeEntryPointUri = SERVER_ROOT_URI + "node";
	        // http://localhost:7474/db/data/node

	        WebResource resource = Client.create()
	                .resource( nodeEntryPointUri);
	        // POST {} to the node entry point URI
	        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
	                .type( MediaType.APPLICATION_JSON )
	                .entity(toJsonNameValuePairCollection(attris))
	                .post( ClientResponse.class );

	        final URI location = response.getLocation();
	        response.close();
	        
	        String[] values = location.toString().split("/");
	        nodeIDs.put(attributes[0], values[values.length-1]);
	        
	        return values[values.length-1];
	        // END SNIPPET: createNode
		}
	}
	
	private static String toJsonNameValuePairCollection(Map<String, String> attris)
    {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		for (Map.Entry<String, String> entry : attris.entrySet()) {
			sb.append(String.format( " \"%s\" : \"%s\" ,", entry.getKey(), entry.getValue() ));
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append("}");
        return sb.toString();
    }

	private static String appendStringByInsertTransaction(String sourceID,
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

	public static void createGraphFromFile(String masterIP, String filePath) {

		SERVER_ROOT_URI = "http://" + masterIP + ":7474/db/data/";

		BufferedReader bReader;
		try {
			bReader = new BufferedReader(new FileReader(new File(filePath)));

			String line = null;

			// initialization
			nodeIDs.clear();
			taskID = 0l;

			StringBuilder sbu = new StringBuilder();
			sbu.append("[");

			int i = 0;

			while ((line = bReader.readLine()) != null) {

				String[] values = line.split(",");
				// sender
//				sbu.append(appendStringByCheckAndInsertAccount(values[1],
//						values[2], values[3], values[4]));
				String senderID = createNodeByCheckAndInsertAccount(values[1],
						values[2], values[3], values[4]);
				// receiver
//				sbu.append(appendStringByCheckAndInsertAccount(values[5],
//						values[6], values[7], values[8]));
				String receiverID = createNodeByCheckAndInsertAccount(values[5],
						values[6], values[7], values[8]);
				// transaction
				sbu.append(appendStringByInsertTransaction(
						senderID, receiverID,
						values[0], values[9], values[10], values[11],
						values[12], values[13], values[14], values[15]));

				if ((++i) > BATCH_MAX_TRANS) {
					i = 0;
					sbu.deleteCharAt(sbu.length() - 1);
					sbu.append("]");
					sendCmd(sbu.toString());
					sbu.delete(0, sbu.length());
					sbu.append("[");
				}

			}
			if (sbu.length() != 0) {
				sbu.deleteCharAt(sbu.length() - 1);
				sbu.append("]");
				sendCmd(sbu.toString());
			}
			bReader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void traverseGraph(String startNodeID, String order,
			int maxDepth) throws URISyntaxException {
		// START SNIPPET: traversalDesc
		// TraversalDefinition turns into JSON to send to the Server
		TraversalDefinition t = new TraversalDefinition();
		t.setOrder(order);
		t.setUniqueness(TraversalDefinition.NODE);
		t.setMaxDepth(maxDepth);
		t.setReturnFilter(TraversalDefinition.ALL);
		//t.setRelationships(new Relation("ATTR_EDGE_TYPE_TRAN", Relation.OUT));
		// END SNIPPET: traversalDesc

		// START SNIPPET: traverse
		URI traverserUri = new URI(SERVER_ROOT_URI + "node/"
				+ startNodeID.toString() + "/traverse/node");
		WebResource resource = Client.create().resource(traverserUri);
		String jsonTraverserPayload = t.toJson();
		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON).entity(jsonTraverserPayload)
				.post(ClientResponse.class);

		System.out.println(String.format(
				"POST [%s] to [%s], status code [%d], returned data: "
						+ System.getProperty("line.separator") + "%s",
				jsonTraverserPayload, traverserUri, response.getStatus(),
				response.getEntity(String.class)));
		response.close();
		// END SNIPPET: traverse
	}

	public static void queryGraph(String nodeID) {

		final String nodeEntryPointUri = SERVER_ROOT_URI + "node/" + nodeID;
		// http://localhost:7474/db/data/node/30

//		WebResource resource = Client.create().resource(nodeEntryPointUri);
//		// POST {} to the node entry point URI
//		ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
//				.type(MediaType.APPLICATION_JSON).entity("{}")
//				.get(ClientResponse.class);
		
		WebResource resource = Client.create()
                .resource( SERVER_ROOT_URI );
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).get( ClientResponse.class );

		System.out.println(String.format(
				"POST to [%s], status code [%d], content [%s]",
				nodeEntryPointUri, response.getStatus(),
				response.getEntity(String.class)));
		response.close();

	}

}
