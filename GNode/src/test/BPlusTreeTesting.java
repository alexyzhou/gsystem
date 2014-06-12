package test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Random;

import data.io.DS_DataType;
import data.io.Data_Schema;
import data.io.Data_Schema.ColumnDescription;
import ds.bplusTree.BPlusTree;

public class BPlusTreeTesting {

	public static void main(String[] args) {
		
		
		schemaTest();
		
	}
	
	public static void schemaTest() {
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
			ObjectOutputStream oo = new ObjectOutputStream(new FileOutputStream(new File("/Users/alex/Documents/dataSchema")));
			oo.writeObject(schema);
			
			oo.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void IndexTest() {
		int vertexCount = 60000;
		int edgeCount = 60000 * 25;
		int nodeCount = 5;
		
		BPlusTree<String, String> globalIndex = new BPlusTree<String, String>(1024);
		ArrayList<BPlusTree<String, String>> localIndex = new ArrayList<>(5);
		
		for (int i = 0; i < nodeCount; i++) {
			localIndex.add(new BPlusTree<String, String>(1024));
		}
		
		Random random = new Random(System.currentTimeMillis());
		
		for (int i = 0; i < 12000; i++) {
			int target = random.nextInt(nodeCount);
			globalIndex.insertOrUpdate(Integer.toString(i), Integer.toString(target));
			//localIndex.get(target).insertOrUpdate(Integer.toString(i), Integer.toString(target));
		}
		
		try {
			ObjectOutputStream oo = new ObjectOutputStream(new FileOutputStream(new File("/Users/alex/Documents/globalIndex")));
			oo.writeObject(globalIndex);
			
			oo.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
