package hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import data.io.Data_Schema;
import data.io.Data_Schema.ColumnDescription;

public class HDFS_Utilities {

	private static HDFS_Utilities _instance = null;

	public static HDFS_Utilities getInstance() throws IOException {
		if (_instance == null) {
			_instance = new HDFS_Utilities();
			_instance.init();
		}
		return _instance;
	}

	private FileSystem fs;

	private void init() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path(System.getenv("HADOOP_HOME")
				+ "/conf/core-site.xml"));
		this.fs = FileSystem.get(conf);
	}

	public void CheckPath_All(String path) throws IOException {
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

	public ArrayList<Path> readChildrenList(String path) throws IOException {
		Path dst = new Path(path);
		if (!fs.exists(dst)) {
			fs.mkdirs(dst);
			return null;
		}
		FileStatus[] status = fs.listStatus(dst);
		ArrayList<Path> fileList = new ArrayList<Path>();
		for (FileStatus st : status) {
			if (fs.isFile(st.getPath())) {
				fileList.add(st.getPath());
			}
		}
		return fileList;
	}

	public String flushObjectToHDFS(String basePath, String fileName, Object obj) {

		try {

			// TODO CheckPath

			String path = basePath + "/" + new Date().getTime();

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

	public Object readFileToObject(String path) {
		Path sre = new Path(path);
		return readFileToObject(sre);
	}

	public boolean deleteFile(String path) {
		Path srd = new Path(path);
		try {
			fs.delete(srd, true);
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public Object readFileToObject(Path sre) {
		try {
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

	public HashMap<String, String> readDataSetByOffset(String hdfsPath, Data_Schema ds, HashMap<Long, List<String>> pointer) {
		HashMap<String, String> result = new HashMap<>();
		try {
			FSDataInputStream inputStream = fs.open(new Path(hdfsPath));
			
			Set<Long> keySet = pointer.keySet();
			for (Long offset: keySet) {
				
				inputStream.seek(offset);
				BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
				String line = br.readLine();
				System.out.println("HDFS Dataset Read line " + line);
				if (ds.getSeperator() != '\0') {
					String[] values = line.split(ds.getSeperator() + "");
					for (int i = 0; i < ds.getColumns().size(); i++) {
						ColumnDescription cd = ds.getColumns().get(i);
						if (pointer.get(offset).contains(cd.name)) {
							//need to be collected
							result.put(cd.name, values[new Integer(cd.range)]);
						}
					}
				} else {
					for (int i = 0; i < ds.getColumns().size(); i++) {
						ColumnDescription cd = ds.getColumns().get(i);
						if (pointer.get(offset).contains(cd.name)) {
							//need to be collected
							String[] ranges = cd.range.split("-");
							result.put(cd.name, line.substring(new Integer(ranges[0]),new Integer(ranges[1])));
						}
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return result;
	}
}
