package com.scistor.process.utils;

import com.scistor.process.distribute.DistributeControl;
import com.scistor.process.distribute.OperatorScheduler;
import com.scistor.process.thrift.service.SlaveService;
import com.scistor.process.utils.params.RunningConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;

public class ClassUpdateHelper implements RunningConfig {

	private static final Log LOG = LogFactory.getLog(ClassUpdateHelper.class);
	
	public static int updateClassLoader (URL[] urls, String componentName, ByteBuffer componentInfo) {
		OperatorScheduler.classLoader = new URLClassLoader(urls,Thread.currentThread().getContextClassLoader());
		//更新从节点的ClassLoader
		int i;
		TFramedTransport transport = null;
		for (i = 0; i < DistributeControl.slavesIP.size(); i++) {
			try {
				transport = new TFramedTransport(new TSocket(DistributeControl.slavesIP.get(i), DistributeControl.slavesPort.get(i), THRIFT_SESSION_TIMEOUT));
				TProtocol protocol = new TCompactProtocol(transport);
				SlaveService.Client client = new SlaveService.Client(protocol);
				transport.open();
				client.addComponent(componentName, componentInfo);
				transport.close();
			} catch (TException e) {
				LOG.error(String.format("Distribute component jar file:[%s] to server:[%s] failed", componentName, DistributeControl.slavesIP.get(i)), e);
			} finally {
				transport.close();
			}
		}
		return 0;
	}
	
}
