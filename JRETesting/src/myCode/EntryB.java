package myCode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

public class EntryB {

	private Boolean test = false;
	private Object test_lock = new Object();

	public static String getTime() {
		SimpleDateFormat format = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss:SSS");
		return format.format(new Date());
	}

	private class ra implements Runnable {

		@Override
		public void run() {
			// TODO Auto-generated method stub
			while (true) {
				synchronized (test_lock) {
					test = true;
					System.out.println(getTime() + "test <=> true");
				}
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					// TODO: handle exception
				}
			}
		}

	}

	private class rb implements Runnable {

		@Override
		public void run() {
			// TODO Auto-generated method stub
			while (true) {
				synchronized (test_lock) {
					test = false;
					System.out.println(getTime() + "test <=> false");
				}
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					// TODO: handle exception
				}
			}

		}
	}

	private class rc implements Runnable {

		@Override
		public void run() {
			// TODO Auto-generated method stub
			while (true) {
				synchronized (test_lock) {
					System.out.println(getTime() + "test = " + test);
				}
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					// TODO: handle exception
				}
			}

		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new FileInputStream(new File("/home/alex/GS2/part-r-00000"))));
			String line=null;
			int num=0;
			while ((line=br.readLine())!=null) {
				String[] values=line.split("\t");
				System.out.println(values[0]);
				System.out.println(values[1]);
				if(++num == 20) {
					br.close();
					return;
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
