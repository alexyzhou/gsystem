package myCode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class NodeSourceParser {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(new File("/home/alex/nodesource")));
			String line = null;
			while((line = br.readLine())!=null) {
				System.out.println(line.substring(6));
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
