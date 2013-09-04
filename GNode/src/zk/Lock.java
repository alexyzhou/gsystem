package zk;
 
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import system.SystemConf;


public class Lock {
    private String path;
    private ZooKeeper zooKeeper;
    public Lock(String path){
        this.path = path;
    }
     

//    public synchronized void lock() throws Exception{
//        Stat stat = zooKeeper.exists(path, false);
//        String data = InetAddress.getLocalHost().getHostAddress()+":lock";
//        zooKeeper.setData(path, data.getBytes(), stat.getVersion());
//    }
    
    public synchronized void lock(String ip) throws Exception{
        Stat stat = zooKeeper.exists(path, false);
        String data = ip+":lock";
        zooKeeper.setData(path, data.getBytes(), stat.getVersion());
    }
     

//    public synchronized void unLock() throws Exception{
//        Stat stat = zooKeeper.exists(path, false);
//        String data = InetAddress.getLocalHost().getHostAddress()+":unlock";
//        zooKeeper.setData(path, data.getBytes(), stat.getVersion());
//    }
    
    public synchronized void unLock(String ip) throws Exception{
        Stat stat = zooKeeper.exists(path, false);
        String data = ip+":unlock";
        zooKeeper.setData(path, data.getBytes(), stat.getVersion());
    }
    
    public synchronized boolean checkAndRecover() {
        try {
            Stat stat = zooKeeper.exists(path, false);
            if (stat == null) {
    			String data = SystemConf.getInstance().localIP + ":unlock";
    			zooKeeper.create(path, data.getBytes(), Collections
    					.singletonList(new ACL(Perms.ALL, Ids.ANYONE_ID_UNSAFE)),
    					CreateMode.EPHEMERAL);
            }
            if (!isLock()) {
            	lock(SystemConf.getInstance().localIP);
            }
        } catch (KeeperException e) {
            //TODO use log system and throw a new exception
        } catch (InterruptedException e) {
            // TODO use log system and throw a new exception
        } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return false;
    }
     

    public synchronized boolean isLock(){
        try {
            Stat stat = zooKeeper.exists(path, false);
            String data = InetAddress.getLocalHost().getHostAddress()+":lock";
            String nodeData = new String(zooKeeper.getData(path, true, stat));
            if(data.equals(nodeData)){
//              lock = true;
                return true;
            }
        } catch (UnknownHostException e) {
            // ignore it
        } catch (KeeperException e) {
            //TODO use log system and throw a new exception
        } catch (InterruptedException e) {
            // TODO use log system and throw a new exception
        }
        return false;
    }
    
    public synchronized boolean isLock(String ip){
        try {
            Stat stat = zooKeeper.exists(path, false);
            String data = ip+":lock";
            String nodeData = new String(zooKeeper.getData(path, true, stat));
            if(data.equals(nodeData)){
//              lock = true;
                return true;
            }
        } catch (KeeperException e) {
            //TODO use log system and throw a new exception
        } catch (InterruptedException e) {
            // TODO use log system and throw a new exception
        }
        return false;
    }
 
    public String getPath() {
        return path;
    }
 
    public void setPath(String path) {
        this.path = path;
    }
 
    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }
     
     
}