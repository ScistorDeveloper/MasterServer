package com.scistor.process.distribute;

import com.google.common.base.Objects;
import com.scistor.process.pojo.SlavesLocation;
import com.scistor.process.pojo.TaskResult;
import com.scistor.process.thrift.service.SlaveService;
import com.scistor.process.utils.ErrorUtil;
import com.scistor.process.utils.ZKOperator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * Created by Administrator on 2017/11/7.
 */
public class DistributeControl implements Runnable{

	private static final Log LOG = LogFactory.getLog(DistributeControl.class);
	private static final int SESSION_TIMEOUT = Integer.MAX_VALUE;
	private static ArrayList<Integer> slavesPort = new ArrayList<Integer>();
	private static ArrayList<String> slavesIP = new ArrayList<String>();
	private String taskId;
	private List<Map<String, String>> mainClass2ElementList;
	private static int index = 0;

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

		List<TaskResult> results = new ArrayList<TaskResult>();
		for (int i = 0; i < mainClass2ElementList.size(); i++) {
			TaskResult result = new TaskResult(taskId, mainClass2ElementList.get(i).get("mainclass"), false, "");
			results.add(result);
		}

		try {
			ZooKeeper zooKeeper = ZKOperator.getZookeeperInstance();
			ZKOperator.initTaskResult(zooKeeper, null, taskId, results, slavesIP, slavesPort);
			zooKeeper.close();
			List<Map<String, String>> userDefinedOperaterList = getUserDefinedOperaterList(mainClass2ElementList);
			final Thread[] threads = new Thread[slavesIP.size()];
			List<FutureTask<String>> futures = new ArrayList<FutureTask<String>>();
			int current = 0;
			for (int i = 0; i < slavesIP.size(); i++) {
				final int slaveNo = i;
				FutureTask<String> future = null;
				if (current < userDefinedOperaterList.size() && index % slavesIP.size() == slaveNo) {
					Map<String, String> curMergeOperator = new HashMap<String, String>();
					curMergeOperator.putAll(userDefinedOperaterList.get(current));
					curMergeOperator.put("task_type", "consumer");
					mainClass2ElementList.add(curMergeOperator);
					future = new FutureTask<String>(new Callable<String>() {
						@Override
						public String call() throws Exception {
							String result = addSubTask(mainClass2ElementList, taskId, slavesIP.get(slaveNo) + ":" + slavesPort.get(slaveNo), slavesIP.get(slaveNo), slavesPort.get(slaveNo), true);
							LOG.info(String.format("result is:[%s]", result));
							return result;
						}
					});
				} else {
					future = new FutureTask<String>(new Callable<String>() {
						@Override
						public String call() throws Exception {
							String result = addSubTask(mainClass2ElementList, taskId, slavesIP.get(slaveNo) + ":" + slavesPort.get(slaveNo), slavesIP.get(slaveNo), slavesPort.get(slaveNo), false);
							return result;
						}
					});
				}
				threads[i] = new Thread(future);
				threads[i].setName(taskId);
				threads[i].start();
				LOG.info(String.format("taskId:[%s], slaveNo:[%s] is processing!", taskId, slaveNo));
				futures.add(future);
				if (current < userDefinedOperaterList.size() && index % slavesIP.size() == slaveNo) {
					mainClass2ElementList.remove(mainClass2ElementList.size() - 1);
					current++;
					index++;
				}
			}
			for (int i = 0; i < slavesIP.size(); i++) {
				if (null != threads[i]) {
					threads[i].join();
				}
			}
		} catch (Exception e) {
			ErrorUtil.ErrorLog(LOG, e);
		}

	}

	private String addSubTask(List<Map<String, String>> elements, String taskId, String slave, String slaveIp, int port, boolean contained) {
		try {
			TFramedTransport transport = new TFramedTransport(new TSocket(slaveIp, port, SESSION_TIMEOUT));
			TProtocol protocol = new TCompactProtocol(transport);
			SlaveService.Client client = new SlaveService.Client(protocol);
			transport.open();
			String slaveResult = client.addSubTask(elements, taskId, slave, contained);
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

	private static void getSlaves() {
		ZooKeeper zookeeper = null;
		try {
			zookeeper = ZKOperator.getZookeeperInstance();
			List<SlavesLocation> slavesLocation = ZKOperator.getLivingSlaves(zookeeper);
			for (SlavesLocation slaveLocation : slavesLocation) {
				slavesIP.add(slaveLocation.getIp());
				slavesPort.add(slaveLocation.getPort());
			}
		} catch (Exception e) {
			ErrorUtil.ErrorLog(LOG, e);
		}finally{
			if(!Objects.equal(zookeeper, null)){
				try {
					zookeeper.close();
				} catch (InterruptedException e) {
					ErrorUtil.ErrorLog(LOG, e);
				}
			}
		}
	}

	private List<Map<String, String>> getUserDefinedOperaterList(List<Map<String, String>> elements) {
		List<Map<String, String>> userDefinedOperaterList = new ArrayList<Map<String, String>>();
		for (Map<String, String> element : elements) {
			if (element.get("type").equals("process")) {
				userDefinedOperaterList.add(element);
			}
		}
		return userDefinedOperaterList;
	}

}