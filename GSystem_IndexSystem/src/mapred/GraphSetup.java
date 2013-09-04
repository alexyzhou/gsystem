package mapred;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import data.io.DS_DataType;
import data.io.Data_Schema;
import data.io.Data_Schema.ColumnDescription;

public class GraphSetup {

	public static class TokenizerMapper

	extends Mapper<LongWritable, Text, Text, LongWritable> {

		// private final static IntWritable one = new IntWritable(1);

		// private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)

		throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//String[] range = conf.getQueryRange();
			String attributeNames=conf.get("queryAttributes");
			String attributeName[]=attributeNames.split("@");
			String seperator = conf.get("querySeperator");
			String content = "";
			for(int j=0;j<attributeName.length;j++){
				String[] range={"",""};				
				 range[0] = conf.get("query"+attributeName[j]+"Range0");
				 range[1] = conf.get("query"+attributeName[j]+"Range1");
			if ((range[1] == null || range[1].equals("")) && !range[0].equals("")) {

//				String[] temp = value.toString().split(
//						conf.getQuerySeperator() + "");
				String[] temp = value.toString().split(
						seperator);
				
				content = content+temp[(Integer.valueOf(range[0]))]+"-";

				// range[0];
			}
			if (range[1] != null && !range[1].equals("")) {
				content = content+value.toString().substring(Integer.valueOf(range[0]),
						Integer.valueOf(range[1]))+"-";
			}
			}
			context.write(new Text(content), key);

			//System.out.print("seperator: " + conf.getQuerySeperator());
			
			

			// if (Long.valueOf(key.toString()) == 0) {
			// StringTokenizer itr = new StringTokenizer(value.toString());
			//
			// while (itr.hasMoreTokens()) {
			//
			// word.set(itr.nextToken());
			//
			// context.write(value, one);
			// }
			// }
		}
	}

	// public static class IntSumReducer
	//
	// extends Reducer<Text, IntWritable, Text, IntWritable> {
	//
	// private IntWritable result = new IntWritable();
	//
	// public void reduce(Text key, Iterable<IntWritable> values,
	// Context context)
	//
	// throws IOException, InterruptedException {
	//
	// int sum = 0;
	//
	// for (IntWritable val : values) {
	//
	// sum += val.get();
	//
	// }
	//
	// result.set(sum);
	//
	// context.write(key, result);
	//
	// }
	//
	// }

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		// conf.setOffset(0);

		// String[] otherArgs = new
		// GenericOptionsParser(conf,args).getRemainingArgs();
		//
		// if (otherArgs.length != 2) {
		//
		// System.err.println("Usage: wordcount <in> <out>");
		//
		// System.exit(2);
		//
		// }
		
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
		String attributeNames = "transaction_id@sender_id@receiver_id";
		String[] attributeName=attributeNames.split("@");
		conf.set("queryAttributes",attributeNames);
		conf.set("querySeperator", schema.getSeperator()+"");
		for(int j=0;j<attributeName.length;j++){
			String[] range = {"",""};
		for (int i = 0; i < columns.size(); i++) {
			if (schema.getColumns().get(i).name.equals(attributeName[j]))
			{
				
				if (schema.getSeperator() == '\0') {
					range = schema.getColumns().get(i).range.split("-");
					conf.set("query"+attributeName[j]+"Range0", range[0]);
					conf.set("query"+attributeName[j]+"Range1", range[1]);
				} else {
					range[0] = schema.getColumns().get(i).range;
					conf.set("query"+attributeName[j]+"Range0", range[0]);
					conf.set("query"+attributeName[j]+"Range1", range[1]);
				}
			}
		}
		}
		Job job = new Job(conf, "word count");

		job.setJarByClass(GraphSetup.class);

		job.setMapperClass(TokenizerMapper.class);

		// job.setCombinerClass(IntSumReducer.class);

		// job.setReducerClass(TokenizerMapper.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(LongWritable.class);

		// FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		//
		// FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
