package test;

import java.util.ArrayList;

import data.io.DS_DataType;
import data.io.Graph_Schema;
import data.io.Graph_Schema.Attribute;

public class TestVariables {
	
	public static String GRAPH_ID = "Graph0001";
	public static String VERTEXSCHEMA_ID = "GSV0001";
	public static String EDGESCHEMA_ID = "GSE0001";
	
	public static String DATASET_ID = "simple";
	public static String DATASET_PATH = "/GNode/simple-graph.csv";
	public static String DATASCHEMA_ID = "DS0001";
	
	public static final String TARGET_IP = "192.168.162.101";
	public static final String MASTER_IP = "192.168.162.100";
	
	public static Graph_Schema get_VertexSchema() {
		Graph_Schema vertexSchema = new Graph_Schema();
		vertexSchema.setsId(TestVariables.VERTEXSCHEMA_ID);
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

	public static Graph_Schema get_EdgeSchema() {
		Graph_Schema edgeSchema = new Graph_Schema();
		edgeSchema.setsId(TestVariables.EDGESCHEMA_ID);
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
