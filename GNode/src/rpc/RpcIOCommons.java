package rpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import system.SystemConf;

public class RpcIOCommons implements Runnable {

	public enum EType {
		INTEGER, STRING, LONG
	}

	public static <E> boolean readCollection(Collection<E> coll, DataInput read)
			throws IOException {
		int num = read.readInt();

		if (num == 0)
			return false;

		EType e;
		E obj = null;
		if (obj instanceof String) {
			e = EType.STRING;
		} else if (obj instanceof Integer) {
			e = EType.INTEGER;
		} else if (obj instanceof Long) {
			e = EType.LONG;
		} else {
			return false;
		}

		for (int i = 0; i < num; i++) {
			switch (e) {
			case STRING:
				coll.add((E) read.readUTF());
				break;
			case INTEGER:
				coll.add((E) (new Integer(read.readInt())));
				break;
			case LONG:
				coll.add((E) (new Long(read.readLong())));
				break;
			default:
				break;
			}
		}
		return true;
	}

	public static <E> boolean writeCollection(Collection<E> coll,
			DataOutput write) throws IOException {
		if (coll.size() == 0) {
			write.writeInt(0);
			return false;
		}

		write.writeInt(coll.size());

		EType e;
		E obj = null;
		if (obj instanceof String) {
			e = EType.STRING;
		} else if (obj instanceof Integer) {
			e = EType.INTEGER;
		} else if (obj instanceof Long) {
			e = EType.LONG;
		} else {
			return false;
		}

		for (E ele : coll) {
			switch (e) {
			case STRING:
				write.writeUTF((String) ele);
				break;
			case INTEGER:
				write.writeInt(((Integer) ele).intValue());
				break;
			case LONG:
				write.writeLong(((Long) ele).longValue());
				break;
			default:
				break;
			}
		}
		return true;
	}

	private static HashMap<String, GServerProtocol> gSProtocolFarm = new HashMap<String, GServerProtocol>();
	private static Object gSProtocolLock = new Object();
	private static GMasterProtocol gMProtocol;
	private static Object gMProtocol_lock = new Object();
	private static HashMap<String, Boolean> gSProtocolFarm_visit = new HashMap<String, Boolean>();
	private static Boolean gMProtocol_visit = false;

	public static GServerProtocol getGServerProtocol(String ip)
			throws IOException {
		if (recycle_Thread == null) {
			recycle_Thread = new Thread(new RpcIOCommons());
			recycle_Thread.start();
		}
		synchronized (gSProtocolLock) {
			if (gSProtocolFarm.get(ip) == null) {
				InetSocketAddress address = new InetSocketAddress(ip,
						SystemConf.getInstance().RPC_GSERVER_PORT);
				GServerProtocol proxy = (GServerProtocol) RPC.waitForProxy(
						GServerProtocol.class, SystemConf.RPC_VERSION, address,
						new Configuration());
				gSProtocolFarm.put(ip, proxy);
				System.out.println("[gSProtocol] "+ip+" is INITIATED");
			}
			synchronized (gSProtocolFarm_visit) {
				gSProtocolFarm_visit.put(ip, new Boolean(true));
			}
			return gSProtocolFarm.get(ip);
		}
	}

	public static GMasterProtocol getMasterProxy() throws IOException {
		if (recycle_Thread == null) {
			recycle_Thread = new Thread(new RpcIOCommons());
			recycle_Thread.start();
		}
		synchronized (gMProtocol_lock) {
			if (gMProtocol == null) {
				InetSocketAddress address = new InetSocketAddress(
						SystemConf.getInstance().masterIP,
						SystemConf.getInstance().RPC_GMASTER_PORT);

				GMasterProtocol proxy = (GMasterProtocol) RPC.waitForProxy(
						GMasterProtocol.class, SystemConf.RPC_VERSION, address,
						new Configuration());
				gMProtocol = proxy;
			}
			synchronized (gMProtocol_visit) {
				gMProtocol_visit = true;
			}
			return gMProtocol;
		}
	}

	protected static void recycleConnections() {
		// Now check MasterProtocol
		synchronized (gMProtocol_visit) {
			if (gMProtocol_visit.equals(false)) {
				synchronized (gMProtocol_lock) {
					if (gMProtocol != null) {
						RPC.stopProxy(gMProtocol);
						gMProtocol = null;
					}
				}
			}
		}
		// Now check gServerProtocol
		synchronized (gSProtocolFarm_visit) {
			synchronized (gSProtocolLock) {
				Set<String> keySet = gSProtocolFarm.keySet();
				ArrayList<String> removeSet = new ArrayList<String>();
				for (String key : keySet) {
					Boolean visit = gSProtocolFarm_visit.get(key);
					if (visit != null && visit.equals(false)) {
						RPC.stopProxy(gSProtocolFarm.get(key));
						removeSet.add(key);
					}
				}
				for (String key : removeSet) {
					System.out.println("[gSProtocol] "+key+" is REMOVED");
					gSProtocolFarm.remove(key);
				}
			}
		}
	}

	protected static void clearVisit() {
		synchronized (gMProtocol_visit) {
			gMProtocol_visit = false;
		}
		synchronized (gSProtocolFarm_visit) {
			synchronized (gSProtocolLock) {
				Set<String> keySet = gSProtocolFarm.keySet();
				for (String key : keySet) {
					gSProtocolFarm_visit.put(key, new Boolean(false));
				}
			}
		}
	}

	private static boolean isRunning = true;
	private static int BEGIN_TIME_INTERVAL = 5 + 1;
	private static int RECYCLE_INTERVAL = 2 + 1;
	private static Thread recycle_Thread = null;

	@Override
	public void run() {
		
		int bCount = 0;
		while (bCount != BEGIN_TIME_INTERVAL) {
			bCount++;
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				
				e.printStackTrace();
			}
		}
		int rCount = 0;
		clearVisit();
		while (isRunning) {
			try {
				rCount++;
				if (rCount == RECYCLE_INTERVAL) {
					rCount = 0;
					recycleConnections();
					clearVisit();
				}
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	public static void stop() {
		isRunning = false;
	}
}
