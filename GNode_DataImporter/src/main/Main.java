package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		conf.set("GNMasterIP", args[0]);
		Job job;
		try {
			job = new Job(conf, "GNode-DataImporter");

	        job.setJarByClass(Main.class);

			job.setMapperClass(ImporterMapper.class);

			job.setOutputKeyClass(Text.class);

			job.setOutputValueClass(Text.class);
			

			FileInputFormat.addInputPath(job, new Path(args[1]));
			//
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
	}

}
