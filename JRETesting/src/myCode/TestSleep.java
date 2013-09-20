package myCode;

import java.io.File;

public class TestSleep {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		 File file = new File("/Users/alex/Documents/touch");
         if(!file.exists())
			try {
				if(!file.createNewFile())
				     throw new Exception("文件不存在，创建失败！");
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
             
		
		for (int i = 0; i < 5; i++) {
			System.out.println("Tick " + i);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
