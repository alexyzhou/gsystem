package code;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class MyServer implements MyProtocol, Runnable {
	
	private Server server;
	private Thread rpcThread;
	
	public static Thread mainThread;
	
	public MyServer() {
	}
	
	public void close() {
		server.stop();
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Text println(Text s) {
		// TODO Auto-generated method stub
		System.out.println("[RPC]"+s);
		return new Text("RPC Finished!");
	}
	
	public static void main(String[] args) {
		mainThread = new Thread(new MyServer());
		mainThread.start();
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		rpcThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				try {
					server = RPC.getServer(MyServer.this, "localhost", 8888, new Configuration());
					server.start();
					//server.join();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} 
			}
		});
		
		rpcThread.start();
	}

	@Override
	public void stopService() {
		// TODO Auto-generated method stub
	}

}
