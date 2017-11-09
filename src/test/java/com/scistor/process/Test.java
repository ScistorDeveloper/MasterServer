package com.scistor.process;

/**
 * Created by Administrator on 2017/11/7.
 */
public class Test {

	public static void main(String[] args) {
//		File file = new File("D:\\AAA");
//		File[] files = file.listFiles(new FilenameFilter() {
//			@Override
//			public boolean accept(File arg0, String arg1) {
//				System.out.println(String.format("arg0:[%s], arg1[%s]", arg0.getName(), arg1));
//				return arg0.getName().endsWith(".jar");
//			}
//		});

		Thread[] threads = new Thread[25];
		for (int i = 0; i < threads.length; i++) {
			threads[i] = new Thread(new HanldMessage());
			threads[i].start();
		}


	}

	static class HanldMessage implements Runnable {
		@Override
		public void run() {
			for(int i=0; i<5000; i++) {
				FlumeClientOperator.sendDataToFlume("this is a client test");
			}
		}
	}

}
