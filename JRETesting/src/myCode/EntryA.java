package myCode;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class EntryA {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {

			String hadoopPath = "/Users/alex/Documents/gsystem/lib/hadoop-1.2.1/bin/hadoop";
			String jarPath = "/Users/alex/Documents/gsystem/lib/GNode/GNode_IndexSetup.jar";
			String inputPath = "/Users/alex/Documents/DATA/out.csv";
			String outputPath = "/Users/alex/Documents/DATA/output/";
			String schemaID = "DS0001";
			String attriName = "sender_id";
			String sourceIP = "127.0.0.1";
			
			
			System.out.println(System.getenv("JAVA_HOME"));

			Process p = Runtime.getRuntime().exec(
					hadoopPath + " jar " + jarPath + " " + inputPath + " "
							+ outputPath + " " + schemaID + " " + attriName
							+ " " + sourceIP);

			BufferedReader br = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
			String line;
			while ((line = br.readLine()) != null)
				System.out.println(line);

			System.out.println("Finished!");

		} catch (java.io.IOException e) {
			System.err.println("IOException " + e.getMessage());
		}
	}
}
