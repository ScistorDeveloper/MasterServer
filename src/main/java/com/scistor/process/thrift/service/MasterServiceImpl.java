package com.scistor.process.thrift.service;

import com.google.common.base.Objects;
import com.scistor.process.distribute.DistributeControl;
import com.scistor.process.distribute.OperatorScheduler;
import com.scistor.process.pojo.Response.OperatorResponse;
import com.scistor.process.pojo.Response.TaskResponse;
import com.scistor.process.thrift.client.ClientTest;
import com.scistor.process.thrift.service.MasterService.Iface;
import com.scistor.process.utils.ClassUpdateHelper;
import com.scistor.process.utils.ErrorUtil;
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
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;


/**
 * Created by Administrator on 2017/11/6.
 */
public class MasterServiceImpl implements RunningConfig, Iface {

	private static final Log LOG = LogFactory.getLog(ClientTest.class);

	private static final ConcurrentHashMap<String,String> ClassToCompenent=new ConcurrentHashMap<String,String>();

	@Override
	public String addOperators(String xmlContent) throws TException {
		LOG.info(String.format("[THRIFT]: Task Accept Successfully, xmlContent: [%s]", xmlContent));
		TaskResponse response = null;
		try{
			response = OperatorScheduler.addNewOperators(xmlContent);
		}catch(Exception e){
			LOG.error("add operator capture an exception:[%s]", e);
		}
		LOG.info(String.format("[THRIFT]: Task Response, esponse: [%s]", response.toJSON()));
		return response.toJSON();
	}

	@Override
	public String removeOperators(List<String> operatorMainClassList) throws TException {
		ZooKeeper zookeeper = null;
		try {
			zookeeper = ZKOperator.getZookeeperInstance();
			List<String> runningOperators = ZKOperator.getRunningOperators(zookeeper);
			for (String operatorMainClass : operatorMainClassList) {
				if (!runningOperators.contains(operatorMainClass)) {
					zookeeper.close();
					return String.format("remove operators failed! Caused by operator:[%s] is not running in the system.", operatorMainClass);
				}
			}
			removeOperator(operatorMainClassList, zookeeper);
		} catch (Exception e) {
			LOG.error("remove operators capture an exception", e);
			return String.format("remove operators capture an exception:[%s]", e);
		} finally {
			try {
				zookeeper.close();
			} catch (InterruptedException e) {
				LOG.error("close zookeeper connection captue an exception", e);
			}
		}
		return "success";
	}

	@Override
	public String registerComponent(String componentName, String mainclass, ByteBuffer componentInfo) throws TException {

		load();

		OperatorResponse response=new OperatorResponse(componentName);
		boolean flag;
		List<String> errorInfo = new ArrayList<String>();
		response.setErrorInfo(errorInfo);

		// classloader校验
		String mainClass = mainclass;
		if(ClassToCompenent.containsKey(mainClass)){
			errorInfo.add(String.format("注册失败,组件[%s],主类[%s] 和平台现有组件[%s]的主类冲突,重命名主类或者是删除已有的同名组件后重新注册", componentName, mainClass, ClassToCompenent.get(mainClass)));
			response.setErrorCode(-200);
			LOG.info(errorInfo);
			return response.toJSON();
		}

		//组件 包名冲突校验
		try {
			if(ClassToCompenent.containsValue(componentName)){
				errorInfo.add(String.format("注册失败,组件[%s],和平台现有组件的名称相同",componentName));
				response.setErrorCode(-201);
				LOG.info(errorInfo);
				return response.toJSON();
			}else{
				ZooKeeper zookeeper = null;
				final CountDownLatch cdl = new CountDownLatch(1);
				try {
					zookeeper=new ZooKeeper(ZOOKEEPER_ADDR, ZK_SESSION_TIMEOUT, new Watcher(){

						@Override
						public void process(WatchedEvent event) {
							if(event.getState() == Event.KeeperState.SyncConnected){
								cdl.countDown();
							}
						}

					});
				} catch (IOException e) {
					LOG.error(e);
					errorInfo.add(String.format("注册失败:组件名称[%s]和主类[%s]可用,但zookeeper[%s]交互失败(系统错误)", componentName, mainClass, ZOOKEEPER_ADDR));
					response.setErrorCode(-2);
					LOG.info(errorInfo);
					return response.toJSON();
				}

				//上传组件信息至ZooKeeper
				flag = ZKOperator.uploadOperatorInfo(zookeeper, cdl, componentName, mainClass);

				if(!flag){
					errorInfo.add(String.format("注册失败:组件名称[%s]和主类[%s]可用,但zookeeper[%s]交互失败(系统错误)", componentName, mainClass, ZOOKEEPER_ADDR));
					response.setErrorCode(-2);
					LOG.info(errorInfo);
					return response.toJSON();
				}

				URL[] urls = null;
				//上传jar包至第三方组件存放路径
				//更新url[]以便从中重构建出最新的classloader
				//异常上传,回滚zk xml删除
				try {
					FileOutputStream fos = new FileOutputStream(COMPONENT_LOCATION + File.separator + componentName + ".jar");
					fos.write(componentInfo.array());
					fos.flush();
					fos.close();
				} catch (IOException e) {
					ZKOperator.deleteZNode(zookeeper, cdl, COMPONENT_LOCATION + "/" + componentName);
					LOG.info(e);
					errorInfo.add(String.format("注册失败:组件[%s],主类[%s]可用,但上传组件至主节点第三方类库错误(系统错误)", componentName , mainClass));
					response.setErrorCode(-3);
					LOG.info(errorInfo);
					return response.toJSON();
				}
				urls = refreshUrls();
				//更新ClassLoader
				if(urls == null || urls.length==0){
					LOG.info("could not get urls from hdfs or nfs,new operator may be invalid...");
				}else{
					ClassUpdateHelper.updateClassLoader(urls, componentName, componentInfo);
				}
			}
			errorInfo.add(String.format("注册成功:组件[%s], 主类[%s]可用", componentName, mainClass));
			response.setErrorCode(0);
			LOG.info(errorInfo);
		} catch (Exception e) {
			ErrorUtil.ErrorLog(LOG, e);
			errorInfo.add(String.format("注册失败:组件[%s],主类[%s]可用,但无法连接zookeeper[%](系统错误)", componentName, mainClass, ZOOKEEPER_ADDR));
			response.setErrorCode(-2);
			LOG.info(errorInfo);
			return response.toJSON();
		}
		ClassToCompenent.put(mainClass, componentName);
		LOG.info("现有组件:"+ClassToCompenent);
		return response.toJSON();
	}

	private void load(){
		synchronized (ClassToCompenent) {
			ZooKeeper zookeeper=null;
			final CountDownLatch cdl=new CountDownLatch(1);
			try {
				zookeeper=new ZooKeeper(ZOOKEEPER_ADDR,ZK_SESSION_TIMEOUT,new Watcher(){

					@Override
					public void process(WatchedEvent event) {
						if(event.getState()== Event.KeeperState.SyncConnected){
							cdl.countDown();
						}
					}
				});
				ClassToCompenent.clear();
				String componentName;//jar包名称
				String mainclass;
				for (String child : zookeeper.getChildren(ZK_COMPONENT_LOCATION, false)){
					componentName = child;
					mainclass = new String(zookeeper.getData(ZK_COMPONENT_LOCATION + "/" + child, false, null));
					ClassToCompenent.put(mainclass, componentName);
					LOG.info("现有组件:"+ClassToCompenent);
				}
			} catch (Exception e) {
				LOG.info(e);
			}finally{
				if(!Objects.equal(zookeeper,null)){
					try {
						zookeeper.close();
					} catch (InterruptedException e) {
						LOG.info(e);
					}
				}
			}
		}
	}

	private URL[] refreshUrls(){
		File[] files = new File(COMPONENT_LOCATION).listFiles();
		URL[] urls = new URL[files.length];
		for(int i=0; i<files.length; i++){
			try {
				urls[i]=files[i].toURI().toURL();
			} catch (MalformedURLException e) {
				ErrorUtil.ErrorLog(LOG, e);
				return null;
			}
		}
		return urls;
	}

	private void removeOperator(List<String> operatorMainClassList, ZooKeeper zookeeper) throws Exception {
		for (String operatorMainClass : operatorMainClassList) {
			//在ZK上查询该算子的consumer运行在哪个节点
			List<String> children = zookeeper.getChildren(ZK_RUNNING_OPERATORS + "/" + operatorMainClass, null);
			String ip_port = children.get(0);
			//在赛思执行该算子consumer阶段的从节点上，结束该算子consumer线程的执行
			stopConsumer(ip_port, operatorMainClass);
			//在太极执行该算子producer阶段的从节点上，结束该算子producer线程的执行
			stopProducer(operatorMainClass);
		}
	}

	private void stopConsumer(String consumerSlaveServerIPAndPort, String operatorMainClass) throws Exception{
		TFramedTransport transport = new TFramedTransport(new TSocket(consumerSlaveServerIPAndPort.split(":")[0], Integer.parseInt(consumerSlaveServerIPAndPort.split(":")[1]), THRIFT_SESSION_TIMEOUT));
		TProtocol protocol = new TCompactProtocol(transport);
		SlaveService.Client client = new SlaveService.Client(protocol);
		transport.open();
		client.removeOperator(operatorMainClass, true);
		transport.close();
	}

	private void stopProducer(String operatorMainClass) throws Exception{
		String[] otherSlaveServersArray = SystemConfig.getString("other_slave_servers").split(",");
		for (String otherSlaveServer : otherSlaveServersArray) {
			TFramedTransport transport = new TFramedTransport(new TSocket(otherSlaveServer.split(":")[0], Integer.parseInt(otherSlaveServer.split(":")[1]), THRIFT_SESSION_TIMEOUT));
			TProtocol protocol = new TCompactProtocol(transport);
			SlaveService.Client client = new SlaveService.Client(protocol);
			transport.open();
			client.removeOperator(operatorMainClass, false);
			transport.close();
		}
	}

}
