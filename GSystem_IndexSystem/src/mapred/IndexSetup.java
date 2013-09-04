package mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import data.io.DS_DataType;
import data.io.Data_Schema;

public class IndexSetup {

	public static class IndexMapper

	extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)

		throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			String range0 = conf.get("queryRange0");
			String range1 = conf.get("queryRange1");
			String seperator = conf.get("querySeperator");
			//System.out.println("Range0:" + range0);
			//System.out.println("Range1:" + range1);
			//System.out.println("Seperator:" + seperator);

			String content = range0;
			if ((range1 == null || range1.equals("")) && !range0.equals("")) {
				//System.out.print("range[0]:" + range0);
				//System.out.print("range[1]:" + range0);

				String[] temp = value.toString().split(seperator);
				//System.out.print("temp[0]" + temp[0]);
				//System.out.print("temp[1]" + temp[1]);
				//System.out.print("temp[2]" + temp[2]);
				content = temp[(Integer.valueOf(range0))];

			}
			if (range1 != null && !range1.equals("")) {
				content = value.toString().substring(Integer.valueOf(range0),
						Integer.valueOf(range1));
			}

			context.write(new Text(content), new Text(key.toString()));
		}
	}
	
	public static class IndexReducer

	extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values, Context context)

		throws IOException, InterruptedException {
			
			System.out.println("Reduce");
			
			String v = "";
			while(values.hasNext()) {
				v+=values.next()+"@";
			}
			
			System.out.println(v);
			context.write(key, new Text(v.substring(0,v.length()-1)));
		}
	}

	/*
	  MapRed Entry 
	  Args0: File that need to create index on 
	  Args1: Output Path
	  Args2: DSchemaID 
	  Args3: AttriName that need to be scanned. 
	  Args4:SourceIP
	 */
	public static void main(String[] args) throws Exception {

		if (args.length != 5) {
			return;
		}

		// Prepare Parameters
		String inputPath = args[0];
		String outputPath = args[1];
		String dSchemaID = args[2];
		String attriName = args[3];
		String sourceIP = args[4];
		// End of Parameters Preparation

		Configuration conf = new Configuration();
		
		Data_Schema schema = new Data_Schema();
		schema.setId("1");
		schema.setSeperator(',');
		ArrayList<Data_Schema.ColumnDescription> columns = new ArrayList<Data_Schema.ColumnDescription>();
		columns.add(schema.new ColumnDescription("transaction_id", DS_DataType.integer,"0", true));
		columns.add(schema.new ColumnDescription("sender_id", DS_DataType.integer,"1", true));
		columns.add(schema.new ColumnDescription("sender_restricted_flag", DS_DataType.bool,"2",false));	
		columns.add(schema.new ColumnDescription("sender_account_creation_time", DS_DataType.integer,"3", true));
		columns.add(schema.new ColumnDescription("sender_email_domain", DS_DataType.string,"4", true));
		columns.add(schema.new ColumnDescription("receiver_id", DS_DataType.integer,"5",true));
		columns.add(schema.new ColumnDescription("receiver_restricted_flag", DS_DataType.bool,"6",false));	
		columns.add(schema.new ColumnDescription("receiver_account_creation_time", DS_DataType.integer,"7", true));
		columns.add(schema.new ColumnDescription("receiver_email_domain", DS_DataType.string,"8", true));
		columns.add(schema.new ColumnDescription("transaction_time", DS_DataType.integer,"9", true));
		columns.add(schema.new ColumnDescription("sender_ip", DS_DataType.integer,"10", true));
		columns.add(schema.new ColumnDescription("reveiver_ip", DS_DataType.integer,"11",true));
		columns.add(schema.new ColumnDescription("tran_amount", DS_DataType.integer,"12",true));
		columns.add(schema.new ColumnDescription("fraud_flag", DS_DataType.bool,"13",false));	
		columns.add(schema.new ColumnDescription("creditcard_id", DS_DataType.integer,"14",true));
		columns.add(schema.new ColumnDescription("creditcard_flag", DS_DataType.bool,"15",false));
		schema.setColumns(columns);
		
		// Trying to obtain the data_Schema
//		InetSocketAddress address = new InetSocketAddress(sourceIP,
//				SystemConf.getInstance().RPC_GSERVER_PORT);
//		GServerProtocol proxy = (GServerProtocol) RPC.waitForProxy(
//				GServerProtocol.class, SystemConf.RPC_VERSION, address,
//				new Configuration());
//		Data_Schema schema = proxy.getDataSchema(dSchemaID);
//		RPC.stopProxy(proxy);

		String[] range = { "", "" };

		for (int i = 0; i < schema.getColumns().size(); i++) {
			if (schema.getColumns().get(i).name.equals(attriName)) {
				System.out.print(schema.getColumns().get(i).name);
				System.out.print(attriName);
				if (schema.getSeperator() == '\0') {
					range = schema.getColumns().get(i).range.split("-");
					conf.set("queryRange0", range[0]);
					conf.set("queryRange1", range[1]);

					System.out.print("no seperator");
				} else {
					range[0] = schema.getColumns().get(i).range;

					conf.set("queryRange0", range[0]);
					conf.set("queryRange1", range[1]);
					conf.set("querySeperator", schema.getSeperator() + "");

					System.out.print("seperator is " + schema.getSeperator());

				}
			}
		}

		Job job = new Job(conf, "GSystem Index Setup [DSPath]@" + args[0]
				+ " [DSchemaID]@" + dSchemaID + " [AttriName]@" + attriName);

		job.setJarByClass(IndexSetup.class);

		job.setMapperClass(IndexMapper.class);
		
		job.setCombinerClass(IndexReducer.class);
		
		job.setReducerClass(IndexReducer.class);

		// job.setCombinerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));

		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
