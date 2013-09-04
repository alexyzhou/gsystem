package myCode;

import java.io.InputStreamReader;
import java.io.LineNumberReader;

public class EntryA {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Process process = Runtime.getRuntime().exec("ls /");

			InputStreamReader ir = new InputStreamReader(
					process.getInputStream());
			LineNumberReader input = new LineNumberReader(ir);

			String line;
			while ((line = input.readLine()) != null) {
				System.out.println(line);
			}
		} catch (java.io.IOException e) {
			System.err.println("IOException " + e.getMessage());
		}
	}

}
