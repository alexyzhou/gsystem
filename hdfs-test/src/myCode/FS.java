package myCode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.omg.CORBA.Environment;

public class FS {
	
	protected void CheckPath_All(FileSystem fs, String path) throws IOException {
		String[] path_elements = path.split("/");
		int lengthCount = 0;
		for (int i = 1; i < path_elements.length; i++) {
			lengthCount += path_elements[i].length() + 1;
			
			String pathToCheck = path.substring(0, lengthCount);
			System.err.println(pathToCheck);
			
			Path dst = new Path(pathToCheck);
			if (!fs.exists(dst)) {
				fs.mkdirs(dst);
			}
		}
	}

	protected String flushObjectToHDFS(FileSystem fs, String basePath, Object obj) {

		try {

			//TODO CheckPath
			
			String path = basePath + "/"
					+ new Date().getTime();
			
			Path dst = new Path(path);

			FSDataOutputStream outputStream = null;
			try {
				outputStream = fs.create(dst);
				ObjectOutputStream out = new ObjectOutputStream(outputStream);
				out.writeObject(obj);
				return path;
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (outputStream != null) {
					outputStream.close();
				}
			}

			FileStatus files[] = fs.listStatus(dst);
			for (FileStatus file : files) {
				System.out.println(file.getPath());
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	protected Object readFileToObject(FileSystem fs,String path) {
		try {
			
			Path sre = new Path(path);

			FSDataInputStream inputStream = null;
			try {
				inputStream = fs.open(sre);
				ObjectInputStream in = new ObjectInputStream(inputStream);
				return in.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (inputStream != null) {
					inputStream.close();
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	protected void createFile(String path, String value) {
		Configuration conf = new Configuration();
		conf.addResource(new Path("core-site.xml"));

		byte[] buff = value.getBytes();

		try {
			FileSystem fs = FileSystem.get(conf);

			Path dst = new Path(path);

			FSDataOutputStream outputStream = null;
			try {
				outputStream = fs.create(dst);
				outputStream.write(buff, 0, buff.length);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (outputStream != null) {
					outputStream.close();
				}
			}

			FileStatus files[] = fs.listStatus(dst);
			for (FileStatus file : files) {
				System.out.println(file.getPath());
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void getFileBlockLocations(String path) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("core-site.xml"));

		FileSystem fs = FileSystem.get(conf);

		Path dst = new Path(path);

		FileStatus fileStatus = fs.getFileStatus(dst);

		BlockLocation[] bls = fs.getFileBlockLocations(fileStatus, 0,
				fileStatus.getLen());

		for (BlockLocation block : bls) {
			System.out.println(Arrays.toString(block.getHosts()) + "\t"
					+ Arrays.toString(block.getNames()) + "\t"
					+ block.getLength() + "\t"
					+ Arrays.toString(block.getTopologyPaths()));
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		FS main = new FS();

		LinkedList<String> linkedList = new LinkedList<String>();
		linkedList.add("123");
		linkedList.add("sdf");
		linkedList.add("EOF");

		
		try {
			String basepath = "/GDB" + "/" + InetAddress.getLocalHost().getHostAddress();
			
			Configuration conf = new Configuration();
			conf.addResource(new Path(System.getenv("HADOOP_HOME") + "/conf/core-site.xml"));
			
			FileSystem fs = FileSystem.get(conf);
			
			main.CheckPath_All(fs, basepath);
			
			String path = main.flushObjectToHDFS(fs, basepath, linkedList);
			
			LinkedList<String> in = (LinkedList<String>) main.readFileToObject(fs, path);
			
			for(String ele:in) {
				System.out.println(ele);
			}
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		//main.createFile("/test", "hello world!\n");

		// try {
		// main.getFileBlockLocations("/GraphPrototype.jar");
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
	}

}
