package com.scistor.process.utils;

import com.google.common.base.Objects;
import com.scistor.process.distribute.TaskScheduler;
import com.scistor.process.pojo.SlavesLocation;
import com.scistor.process.thrift.service.SlaveService;
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
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ClassUpdateHelper implements RunningConfig {

	private static final Log LOG = LogFactory.getLog(ClassUpdateHelper.class);
	public static List<Integer> slavesPort = new ArrayList<Integer>();
	public static List<String> slavesIP = new ArrayList<String>();
	
	public static int updateClassLoader (URL[] urls, String componentLocation, String componentName, ByteBuffer componentInfo) {
		TaskScheduler.classLoader = new URLClassLoader(urls,Thread.currentThread().getContextClassLoader());
		//更新从节点的ClassLoader
		getSlaves();
		int i;
		for (i = 0; i < slavesIP.size(); i++) {
			try {
				TFramedTransport transport = new TFramedTransport(new TSocket(slavesIP.get(i), slavesPort.get(i), THRIFT_SESSION_TIMEOUT));
				TProtocol protocol = new TCompactProtocol(transport);
				SlaveService.Client client = new SlaveService.Client(protocol);
				transport.open();
				String result = client.updateClassLoader(componentLocation, componentName, componentInfo);
				if (result.equals("-100")) {
					return -100;
				}
				transport.close();
			} catch (TException e) {
				LOG.error(String.format("Distribute component jar file:[%s] to server:[%s] failed", componentName, slavesIP.get(i)));
				ErrorUtil.ErrorLog(LOG, e);
			}
		}
		return 0;
	}

	private static void getSlaves() {
		ZooKeeper zookeeper = null;
		final CountDownLatch cdl = new CountDownLatch(1);
		try {
			zookeeper = new ZooKeeper(ZOOKEEPER_ADDR, RunningConfig.ZK_SESSION_TIMEOUT, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					if (event.getState() == Event.KeeperState.SyncConnected) {
						cdl.countDown();
					}
				}
			});
		} catch (IOException e) {
			ErrorUtil.ErrorLog(LOG, e);
		}
		try {
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
	
}
