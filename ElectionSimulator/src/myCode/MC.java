package myCode;

import java.util.HashMap;

public class MC {

	public static HashMap<Integer,Thread> map = new HashMap<Integer,Thread>();
	
	public static void suspendThread(int id) {
		(map.get(new Integer(id))).suspend();
	}
	public static void resumeThread(int id) {
		(map.get(new Integer(id))).resume();
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		for (int i = 0; i < 5; i++) {
			Thread th = new Thread(new ServerRunnable(i));
			map.put(new Integer(i), th);
			th.start();
		}
	}

}
