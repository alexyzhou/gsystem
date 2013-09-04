package test;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import rpc.GMasterProtocol;
import rpc.GServerProtocol;
import system.SystemConf;

public class TestClient {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		InetSocketAddress address = new InetSocketAddress("10.60.0.222",SystemConf.getInstance().RPC_GSERVER_PORT);
		try {
			GServerProtocol gMprotocol = (GServerProtocol) RPC.waitForProxy(GMasterProtocol.class, 1, address, new Configuration());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
