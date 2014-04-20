package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GNodeDataImporterMain extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 4) {
			System.out.printf("Three parameters are required: <DatasetID> <NLine> <input dir> <output dir>\n");
			return -1;
		}
		
		Configuration conf = getConf();
		conf.set("GNDSID", args[0]);
		
		Job job = new Job(conf);
		job.setJobName("GNode-DataImporter");
		job.setJarByClass(GNodeDataImporterMain.class);
 
		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job, new Path(args[2]));
		NLineInputFormat.setNumLinesPerSplit(job, Integer.parseInt(args[1]));
//		job.getConfiguration().setInt(
//				"mapreduce.input.lineinputformat.linespermap", );
 
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
 
		job.setMapperClass(ImporterMapper.class);
		job.setNumReduceTasks(0);
 
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new GNodeDataImporterMain(), args);
		System.exit(exitCode);
	}

}
