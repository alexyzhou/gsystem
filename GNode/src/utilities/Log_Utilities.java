package utilities;

import system.SystemConf;

public class Log_Utilities {
	
	public static String LOG_HEADER_DEBUG = "DEBUG";
	public static String LOG_HEADER_INFO = "INFO";

	public static String genGServerLog(String header, String log) {
		return "[" + SystemConf.getTime() + "][gSERVER][" + header + "] " + log;
	}
	
	public static String genGMasterLog(String header, String log) {
		return "[" + SystemConf.getTime() + "][MASTER][" + header + "] " + log;
	}

}
