package system;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import rpc.GMasterProtocol;
import rpc.GServerProtocol;
import utilities.Net_Utilities;

public class StopService {
	
	protected static final String MASTER_IP = "server.master.ip";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Properties prop = new Properties();
		try {
			InputStream in = new FileInputStream("systemconf.properties");
			prop.load(in);
			
			//String masterIP = prop.getProperty(MASTER_IP);
			
			SystemConf.getInstance().localIP = Net_Utilities.obtainLocalIP();
			
			InetSocketAddress address;
			
			address = new InetSocketAddress("10.60.162.100",SystemConf.getInstance().RPC_GMASTER_PORT);
			GMasterProtocol gMprotocol = (GMasterProtocol) RPC.waitForProxy(GMasterProtocol.class, 0, address, new Configuration());
			gMprotocol.stopService();

			for (int i = 101; i < 103; i++) {
				address = new InetSocketAddress("10.60.0."+i,SystemConf.getInstance().RPC_GSERVER_PORT);
				GServerProtocol gSprotocol = (GServerProtocol) RPC.waitForProxy(GServerProtocol.class, SystemConf.RPC_VERSION, address, new Configuration());
				gSprotocol.stopService();
			}
			
			System.out.println("Finished!");
			
			
//			if (SystemConf.getInstance().localIP.equals(masterIP)) {
//				InetSocketAddress address = new InetSocketAddress(SystemConf.getInstance().localIP,SystemConf.getInstance().RPC_GMASTER_PORT);
//				GMasterProtocol gMprotocol = (GMasterProtocol) RPC.waitForProxy(GMasterProtocol.class, 0, address, new Configuration());
//				gMprotocol.stopService();
//			} else {
//				InetSocketAddress address = new InetSocketAddress(SystemConf.getInstance().localIP,SystemConf.getInstance().RPC_GSERVER_PORT);
//				GServerProtocol gMprotocol = (GServerProtocol) RPC.waitForProxy(GServerProtocol.class, SystemConf.RPC_VERSION, address, new Configuration());
//				gMprotocol.stopService();
//			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
