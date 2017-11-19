package com.scistor.process.distribute;

import com.scistor.process.thrift.service.SlaveService;
import com.scistor.process.utils.ZKOperator;
import com.scistor.process.utils.params.RunningConfig;
import com.scistor.process.utils.params.SystemConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/11/7.
 */
public class DistributeControl implements Runnable, RunningConfig{

	private static final Log LOG = LogFactory.getLog(DistributeControl.class);
	private static final int SESSION_TIMEOUT = Integer.MAX_VALUE;
	private static List<String> otherSlavesIP = new ArrayList<String>();
	private static List<Integer> otherSlavesPort = new ArrayList<Integer>();
	private static List<String> scistorSlavesIP = new ArrayList<String>();
	private static List<Integer> scistorSlavesPort = new ArrayList<Integer>();
	public static List<String> slavesIP = new ArrayList<String>();
	public static List<Integer> slavesPort = new ArrayList<Integer>();
	private List<Map<String, String>> mainClass2ElementList;
	private static Integer NUMBER = 0;
	private static ZooKeeper zookeeper;

	static {
		//读取配置文件，将太极从节点信息和赛思从节点信息分别放入对应的list中
		getSlaves();
		try {
			zookeeper = ZKOperator.getZookeeperInstance();
		} catch (IOException e) {
			LOG.error("Get zk instance error", e);
		}
	}

	public DistributeControl(List<Map<String, String>> mainClass2ElementList) {
		super();
		this.mainClass2ElementList = mainClass2ElementList;
	}

	@Override
	public void run() {
		try {
			//将所有的producer算子发送到太极从节点中
			sendProducerToOhterSlaveServers();

			//向zookeeper上注册本次添加的算子
			registerRunningOperators();

			//将所有的consumer算子发送到赛思从节点中
			sendConsumerToScistorSlaveServers();

			zookeeper.close();
		} catch (Exception e) {
			LOG.error("添加新算子出现异常", e);
		}
	}

	private void sendProducerToOhterSlaveServers() {
		for (int i = 0; i < otherSlavesIP.size(); i++) {
			final int index = i;
			Thread thread = new Thread(new Runnable() {
				@Override
				public void run() {
					addNewOperators(mainClass2ElementList, otherSlavesIP.get(index), otherSlavesPort.get(index), false);
				}
			});
			thread.start();
		}
	}

	private void sendConsumerToScistorSlaveServers() throws KeeperException, InterruptedException {
		final int size = scistorSlavesIP.size();
		for (int i = 0; i < mainClass2ElementList.size(); i++) {
			Map<String, String> operator = mainClass2ElementList.get(i);
			String mainClass = operator.get("mainclass");
			operator.put("task_type", "consumer");
			final List<Map<String, String>> list = new ArrayList<Map<String, String>>();
			list.add(operator);
			Thread thread = new Thread(new Runnable() {
				@Override
				public void run() {
					synchronized (NUMBER) {
						addNewOperators(list, scistorSlavesIP.get(NUMBER % size), scistorSlavesPort.get(NUMBER % size), true);
						NUMBER++;
					}
				}
			});
			thread.start();
			ZKOperator.createZnode(zookeeper, ZK_RUNNING_OPERATORS + "/" + mainClass + "/" + scistorSlavesIP.get(NUMBER % size) + ":" + scistorSlavesPort.get(NUMBER % size), "", null);
		}
	}

	private void registerRunningOperators() throws IOException, KeeperException, InterruptedException {
		for (Map<String, String> operator : mainClass2ElementList) {
			String mainClass = operator.get("mainclass");
			ZKOperator.createZnode(zookeeper, ZK_RUNNING_OPERATORS + "/" + mainClass, "-", null);
		}
	}

	private String addNewOperators(List<Map<String, String>> elements, String ip, int port, boolean contained) {
		try {
			TFramedTransport transport = new TFramedTransport(new TSocket(ip, port, SESSION_TIMEOUT));
			TProtocol protocol = new TCompactProtocol(transport);
			SlaveService.Client client = new SlaveService.Client(protocol);
			transport.open();
			client.addOperators(elements,contained);
			transport.close();
			LOG.info(String.format("send new operators to slave[%s:%s] success", ip, port));
			return "success";
		} catch (TException e) {
			LOG.error(String.format("send new operators to slave[%s:%s] capture an exception", ip, port), e);
			return String.format("send new operators to slave[%s:%s] capture an exception", ip, port);
		}
	}

	private static void getSlaves() {
		String[] otherSlaveServerses = SystemConfig.getString("other_slave_servers").split(",");
		String[] scistorSlaveServerses = SystemConfig.getString("scistor_slave_servers").split(",");
		for (String otherSlaveServer : otherSlaveServerses) {
			String[] ip_port = otherSlaveServer.split(":");
			otherSlavesIP.add(ip_port[0]);
			otherSlavesPort.add(Integer.parseInt(ip_port[1]));
		}
		for (String scistorSlaveServerse : scistorSlaveServerses) {
			String[] ip_port = scistorSlaveServerse.split(":");
			scistorSlavesIP.add(ip_port[0]);
			scistorSlavesPort.add(Integer.parseInt(ip_port[1]));
		}
		slavesIP.addAll(otherSlavesIP);
		slavesIP.addAll(scistorSlavesIP);
		slavesPort.addAll(otherSlavesPort);
		slavesPort.addAll(scistorSlavesPort);
	}

}