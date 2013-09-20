package system;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class CpuUsage {
	
	public double cpuUsage;
	public double memUsage;
	
	public CpuUsage(double cu, double mu) {
		this.cpuUsage = cu;
		this.memUsage = mu;
	}
	
	public static CpuUsage getUsage() throws Exception {
		double cpuUsed = 0;
		double memUsed = 0;
		Runtime rt = Runtime.getRuntime();
		Process p = rt.exec("top -b -n 1");// 调用系统的“top"命令
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String str = null;
			String[] strArray = null;
			while ((str = in.readLine()) != null) {
				int m = 0;
				if (str.indexOf(" R ") != -1 && str.indexOf("top") == -1) {// 只分析正在运行的进程，top进程本身除外
					strArray = str.split(" ");
					for (String tmp : strArray) {
						if (tmp.trim().length() == 0)
							continue;
						if (++m == 9) {// 第9列为CPU的使用百分比(RedHat 9)
							cpuUsed += Double.parseDouble(tmp);
						}
					}
				}
				m = 0;
				if (str.indexOf("top") == -1) {//top进程本身除外
					strArray = str.split(" ");
					for (String tmp : strArray) {
						if (tmp.trim().length() == 0)
							continue;
						if (++m == 10) {// 第9列为MEM的使用百分比(RedHat 9)
							if (!tmp.equals("%MEM")) {
								memUsed += Double.parseDouble(tmp);
							}
							
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			in.close();
		}
		return new CpuUsage(cpuUsed, memUsed);
	}
}
