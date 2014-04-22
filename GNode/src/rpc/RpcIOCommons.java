package rpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import system.SystemConf;
import utilities.Lock_Utilities;
import utilities.Log_Utilities;

public class RpcIOCommons implements Runnable {

	public enum EType {
		INTEGER, STRING, LONG
	}

	protected enum VisitStatus {
		OCCUPIED, VISITED, FREE
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
	// private static Object gSProtocolLock = new Object();
	private static GMasterProtocol gMProtocol;
	// private static Object gMProtocol_lock = new Object();
	private static HashMap<String, VisitStatus> gSProtocolFarm_visit = new HashMap<String, VisitStatus>();
	private static VisitStatus gMProtocol_visit = VisitStatus.FREE;

	public static GServerProtocol getGServerProtocol(String ip)
			throws IOException {
		if (recycle_Thread == null) {
			recycle_Thread = new Thread(new RpcIOCommons());
			recycle_Thread.start();
		}

		
		Lock_Utilities.ObtainReadWriteLock("gSProxyLock").writeLock().lock();

		if (gSProtocolFarm.get(ip) == null) {
			InetSocketAddress address = new InetSocketAddress(ip,
					SystemConf.getInstance().RPC_GSERVER_PORT);
			GServerProtocol proxy = (GServerProtocol) RPC.waitForProxy(
					GServerProtocol.class, SystemConf.RPC_VERSION, address,
					new Configuration());
			gSProtocolFarm.put(ip, proxy);
			System.out.println("[gSProtocol] " + ip + " is INITIATED");
		}

		Lock_Utilities.ObtainReadWriteLock("gSProxyLock").writeLock().unlock();

		Lock_Utilities.ObtainReadWriteLock("gSVisitLock").writeLock().lock();

		gSProtocolFarm_visit.put(ip, VisitStatus.OCCUPIED);

		Lock_Utilities.ObtainReadWriteLock("gSVisitLock").writeLock().unlock();

		Lock_Utilities.ObtainReadWriteLock("gSProxyLock").readLock().lock();

		GServerProtocol result = gSProtocolFarm.get(ip);

		Lock_Utilities.ObtainReadWriteLock("gSProxyLock").readLock().unlock();

		return result;
	}

	public static void freeGServerProtocol(String ip) {

		Lock_Utilities.ObtainReadWriteLock("gSProxyLock").readLock().lock();

		if (gSProtocolFarm.get(ip) == null) {
			return;
		}

		Lock_Utilities.ObtainReadWriteLock("gSProxyLock").readLock().unlock();

		Lock_Utilities.ObtainReadWriteLock("gSVisitLock").writeLock().lock();
		if (gSProtocolFarm_visit.get(ip).equals(VisitStatus.OCCUPIED)) {
			gSProtocolFarm_visit.put(ip, VisitStatus.VISITED);
		}
		Lock_Utilities.ObtainReadWriteLock("gSVisitLock").writeLock().unlock();

	}

	public static GMasterProtocol getMasterProxy() throws IOException {
		if (recycle_Thread == null) {
			recycle_Thread = new Thread(new RpcIOCommons());
			recycle_Thread.start();
		}

		Lock_Utilities.ObtainReadWriteLock("gMProxyLock").writeLock().lock();
		

		if (gMProtocol == null) {
			InetSocketAddress address = new InetSocketAddress(
					SystemConf.getInstance().masterIP,
					SystemConf.getInstance().RPC_GMASTER_PORT);

			GMasterProtocol proxy = (GMasterProtocol) RPC.waitForProxy(
					GMasterProtocol.class, SystemConf.RPC_VERSION, address,
					new Configuration());
			gMProtocol = proxy;
		}

		Lock_Utilities.ObtainReadWriteLock("gMProxyLock").writeLock().unlock();

		Lock_Utilities.ObtainReadWriteLock("gMVisitLock").writeLock().lock();

		gMProtocol_visit = VisitStatus.OCCUPIED;

		Lock_Utilities.ObtainReadWriteLock("gMVisitLock").writeLock().unlock();

		return gMProtocol;

	}

	public static void freeMasterProxy() {

		Lock_Utilities.ObtainReadWriteLock("gMProxyLock").readLock().lock();

		if (gMProtocol == null) {
			return;
		}

		Lock_Utilities.ObtainReadWriteLock("gMProxyLock").readLock().unlock();

		Lock_Utilities.ObtainReadWriteLock("gMVisitLock").writeLock().lock();

		if (gMProtocol_visit.equals(VisitStatus.OCCUPIED))
			gMProtocol_visit = VisitStatus.VISITED;

		Lock_Utilities.ObtainReadWriteLock("gMVisitLock").writeLock().unlock();
	}

	protected static void recycleConnections() {
		// Now check MasterProtocol
		Lock_Utilities.ObtainReadWriteLock("gMVisitLock").readLock().lock();
			if (gMProtocol_visit.equals(VisitStatus.FREE)) {
				Lock_Utilities.ObtainReadWriteLock("gMProxyLock").writeLock().lock();
					if (gMProtocol != null) {
						RPC.stopProxy(gMProtocol);
						gMProtocol = null;
					}
				Lock_Utilities.ObtainReadWriteLock("gMProxyLock").writeLock().unlock();
			}
		Lock_Utilities.ObtainReadWriteLock("gMVisitLock").readLock().unlock();
		
		// Now check gServerProtocol
		Lock_Utilities.ObtainReadWriteLock("gSVisitLock").readLock().lock();
			Lock_Utilities.ObtainReadWriteLock("gSProxyLock").readLock().lock();
				Set<String> keySet = gSProtocolFarm.keySet();
				ArrayList<String> removeSet = new ArrayList<String>();
				for (String key : keySet) {
					VisitStatus visit = gSProtocolFarm_visit.get(key);
					if (visit != null && visit.equals(VisitStatus.FREE)) {
						RPC.stopProxy(gSProtocolFarm.get(key));
						removeSet.add(key);
					}
				}
				Lock_Utilities.ObtainReadWriteLock("gSProxyLock").readLock().unlock();
				Lock_Utilities.ObtainReadWriteLock("gSProxyLock").writeLock().lock();
				for (String key : removeSet) {
					System.out.println("[gSProtocol] " + key + " is REMOVED");
					gSProtocolFarm.remove(key);
				}
			Lock_Utilities.ObtainReadWriteLock("gSProxyLock").writeLock().unlock();
		Lock_Utilities.ObtainReadWriteLock("gSVisitLock").readLock().unlock();
	}

	protected static void clearVisit() {
		Lock_Utilities.ObtainReadWriteLock("gMVisitLock").writeLock().lock();
			if (gMProtocol_visit.equals(VisitStatus.VISITED))
				gMProtocol_visit = VisitStatus.FREE;
		Lock_Utilities.ObtainReadWriteLock("gMVisitLock").writeLock().unlock();
		
		Lock_Utilities.ObtainReadWriteLock("gSVisitLock").writeLock().lock();
			Lock_Utilities.ObtainReadWriteLock("gSProxyLock").readLock().lock();
				Set<String> keySet = gSProtocolFarm.keySet();
				for (String key : keySet) {
					if (gSProtocolFarm_visit.get(key).equals(
							VisitStatus.VISITED)) {
						gSProtocolFarm_visit.put(key, VisitStatus.FREE);
					}
				}
			Lock_Utilities.ObtainReadWriteLock("gSProxyLock").readLock().unlock();
		Lock_Utilities.ObtainReadWriteLock("gSVisitLock").writeLock().unlock();
		
	}

	private static boolean isRunning = true;
	private static int BEGIN_TIME_INTERVAL = 10 + 1;
	private static int RECYCLE_INTERVAL = 10 + 1;
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
