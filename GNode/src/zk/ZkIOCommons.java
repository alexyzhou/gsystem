package zk;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;

import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import system.SystemConf;
import data.io.Data_Schema;
import data.io.GraphSchemaCollectionSerializable;

public class ZkIOCommons {
	/**
	 * 序列化
	 * 
	 * @param object
	 * @return
	 */
	public static byte[] serialize(Object object) {
		ObjectOutputStream oos = null;
		ByteArrayOutputStream baos = null;
		try {
			// 序列化
			baos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(baos);
			oos.writeObject(object);
			byte[] bytes = baos.toByteArray();
			return bytes;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 反序列化
	 * 
	 * @param bytes
	 * @return
	 */
	public static Object unserialize(byte[] bytes) {
		ByteArrayInputStream bais = null;
		try {
			// 反序列化
			bais = new ByteArrayInputStream(bytes);
			ObjectInputStream ois = new ObjectInputStream(bais);
			return ois.readObject();
		} catch (Exception e) {

		}
		return null;
	}

	/**
	 * 
	 * 
	 * @param 
	 * @return
	 */
	public static boolean setSchemaFile(ZooKeeper zk, String graph_id,
			GraphSchemaCollectionSerializable gsc) {

		String path = SystemConf.getInstance().zoo_basePath_gSchema + "/"
				+ graph_id;

		org.apache.zookeeper.data.Stat stat;
		try {
			stat = zk.exists(path, null);
			if (stat == null) {
				zk.create(path, serialize(gsc), Collections.singletonList(new ACL(
						Perms.ALL, Ids.ANYONE_ID_UNSAFE)), CreateMode.PERSISTENT);
			} else {
				zk.setData(path, ZkIOCommons.serialize(gsc), -1);
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public static void checkPath_All(ZooKeeper zk, String path) {
		
		String[] path_elements = path.split("/");
		int lengthCount = 0;
		for (int i = 1; i < path_elements.length; i++) {
			lengthCount += path_elements[i].length() + 1;

			String pathToCheck = path.substring(0, lengthCount);

			try {
				if (zk.exists(pathToCheck, false) == null) {
					zk.create(pathToCheck, new String("").getBytes(),
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		

	}
	
	/**
	 * 
	 * 
	 * @param 
	 * @return
	 */
	public static boolean setDSSchemaFile(ZooKeeper zk, String dsSchemaId,
			Data_Schema gsc) {

		String path = SystemConf.getInstance().zoo_basePath_dSchema + "/"
				+ dsSchemaId;

		org.apache.zookeeper.data.Stat stat;
		try {
			stat = zk.exists(path, null);
			if (stat == null) {
				zk.create(path, serialize(gsc), Collections.singletonList(new ACL(
						Perms.ALL, Ids.ANYONE_ID_UNSAFE)), CreateMode.PERSISTENT);
			} else {
				zk.setData(path, ZkIOCommons.serialize(gsc), -1);
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public static boolean removeZKFile(ZooKeeper zk, String Path) {
		org.apache.zookeeper.data.Stat stat;
		try {
			stat = zk.exists(Path, null);
			if (stat != null) {
				zk.delete(Path, stat.getVersion());
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}

}
