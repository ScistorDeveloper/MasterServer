package com.scistor.process.distribute;

import com.google.common.base.Objects;
import com.scistor.process.pojo.SlavesLocation;
import com.scistor.process.thrift.service.SlaveService;
import com.scistor.process.utils.ZKOperator;
import com.scistor.process.utils.params.RunningConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;

/**
 * Created by Administrator on 2017/11/7.
 */
public class DistributeControl implements Runnable{

	private static final Log LOG = LogFactory.getLog(DistributeControl.class);
	private static final String ZK_SERVERS =RunningConfig.ZOOKEEPER_ADDR;
	private static final int SESSION_TIMEOUT = Integer.MAX_VALUE;
	private static ArrayList<Integer> slavesPort = new ArrayList<Integer>();
	private static ArrayList<String> slavesIP = new ArrayList<String>();
	private String taskId;
	private List<Map<String, String>> mainClass2ElementList;
	private static int current = 0;

	public DistributeControl(String taskId, List<Map<String, String>> mainClass2ElementList) {
		super();
		this.taskId = taskId;
		this.mainClass2ElementList = mainClass2ElementList;
	}

	@Override
	public void run() {

		if(TaskScheduler.flag == false) {
			getSlaves();
			TaskScheduler.flag = true;
		}

		List<String> slaveSocket = new ArrayList<String>();
		for (int i = 0; i < slavesIP.size(); i++) {
			slaveSocket.add(slavesIP.get(i) + ":" + slavesPort.get(i));
		}

		try {
			final Thread[] threads = new Thread[slavesIP.size()];
			List<FutureTask<String>> futures = new ArrayList<FutureTask<String>>();
			for (int i = 0; i < slavesIP.size(); i++) {
				final int slaveNo = i;
				FutureTask<String> future = new FutureTask<String>(new Callable<String>() {
					@Override
					public String call() throws Exception {
						String result = addSubTask(mainClass2ElementList, taskId, slaveNo, slavesIP.get(slaveNo), slavesPort.get(slaveNo));
						return result;
					}
				});
				threads[i] = new Thread(future);
				threads[i].setName(taskId);
				threads[i].start();
				LOG.info(String.format("taskId:[%s], slaveNo:[%s] is processing!", taskId, slaveNo));
				futures.add(future);
			}
//			final Thread[] mergeThreads = new Thread[getUserDefinedOperaterNumbers(mainClass2ElementList)];
//			for (final Map<String, String> mainClass2Element : mainClass2ElementList) {
//				final int slaveNo = current % slavesIP.size();
//				int i = 0;
//				if (mainClass2Element.get("type").equals("process")) {
//					FutureTask<String> future = new FutureTask<String>(new Callable<String>() {
//						@Override
//						public String call() throws Exception {
//							String result = addSubMergeTask(mainClass2Element, taskId, slaveNo, slavesIP.get(slaveNo), slavesPort.get(slaveNo));
//							return result;
//						}
//					});
//					mergeThreads[i] = new Thread(future);
//					mergeThreads[i].setName(taskId);
//					mergeThreads[i].start();
//					LOG.info(String.format("taskId:[%s], slaveNo:[%s] is processing!", taskId, slaveNo));
//					futures.add(future);
//					i++;
//					current++;
//				}
//			}
			for (int i = 0; i < slavesIP.size(); i++) {
				if (null != threads[i]) {
					threads[i].join();
				}
			}
//			for (int i = 0; i < mergeThreads.length; i++) {
//				if (null != mergeThreads[i]) {
//					mergeThreads[i].join();
//				}
//			}
		} catch (Exception e) {
			StackTraceElement[] stackTraceElements = e.getStackTrace();
			for (StackTraceElement stackTraceElement : stackTraceElements) {
				LOG.error(stackTraceElement);
			}
		}

	}

	private String addSubTask(List<Map<String, String>> elements, String taskId, int slaveNo, String slaveIp, int port) {
		try {
			TFramedTransport transport = new TFramedTransport(new TSocket(slaveIp, port, SESSION_TIMEOUT));
			TProtocol protocol = new TCompactProtocol(transport);
			SlaveService.Client client = new SlaveService.Client(protocol);
			transport.open();
			String slaveResult = client.addSubTask(elements, taskId, slaveNo, true);
			transport.close();
			return slaveResult;
		} catch (TException e) {
			LOG.error(String.format("fatal error when master schedule task to slave, ip:[%s], port:[%s]", slaveIp, port));
			LOG.error(e);
			for(StackTraceElement ste:e.getStackTrace()){
				LOG.info(ste);
			}
		}
		return "";
	}

//	private String addSubMergeTask(Map<String, String> element, String taskId, int slaveNo, String slaveIp, int port) {
//		try {
//			TFramedTransport transport = new TFramedTransport(new TSocket(slaveIp, port, SESSION_TIMEOUT));
//			TProtocol protocol = new TCompactProtocol(transport);
//			SlaveService.Client client = new SlaveService.Client(protocol);
//			transport.open();
//			String slaveResult = client.addSubMergeTask(element, taskId, slaveNo);
//			transport.close();
//			return slaveResult;
//		} catch (TException e) {
//			LOG.error(String.format("fatal error when master schedule merge task to slave, ip:[%s], port:[%s]", slaveIp, port));
//			LOG.error(e);
//			for(StackTraceElement ste:e.getStackTrace()){
//				LOG.info(ste);
//			}
//		}
//		return "";
//	}

	private static void getSlaves() {
		ZooKeeper zookeeper = null;
		final CountDownLatch cdl = new CountDownLatch(1);
		try {
			zookeeper = new ZooKeeper(ZK_SERVERS, RunningConfig.ZK_SESSION_TIMEOUT, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					if (event.getState() == Event.KeeperState.SyncConnected) {
						cdl.countDown();
					}
				}
			});
		} catch (IOException e) {
			LOG.error(e);
		}
		try {
			List<SlavesLocation> slavesLocation = ZKOperator.getLivingSlaves(zookeeper);
			for (SlavesLocation slaveLocation : slavesLocation) {
				slavesIP.add(slaveLocation.getIp());
				slavesPort.add(slaveLocation.getPort());
			}
		} catch (Exception e) {
			LOG.error(e);
			for(StackTraceElement ste:e.getStackTrace()){
				LOG.error(ste);
			}
		}finally{
			if(!Objects.equal(zookeeper, null)){
				try {
					zookeeper.close();
				} catch (InterruptedException e) {
					LOG.error(e);
				}
			}
		}
	}

	private int getUserDefinedOperaterNumbers(List<Map<String, String>> elements) {
		int total = 0;
		for (Map<String, String> element : elements) {
			if (element.get("type").equals("process")) {
				total ++;
			}
		}
		return total;
	}

}
