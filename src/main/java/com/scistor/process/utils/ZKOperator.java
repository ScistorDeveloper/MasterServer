package com.scistor.process.utils;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Objects;
import com.scistor.process.pojo.SlavesLocation;
import com.scistor.process.pojo.TaskResult;
import com.scistor.process.utils.params.RunningConfig;
import com.scistor.process.utils.params.SystemConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * 
 * @description  zookeeper 操作类
 * @author zhujiulong
 * @date 2016年7月21日 上午11:42:47
 *
 */
public class ZKOperator implements RunningConfig {

	private static final Log LOG = LogFactory.getLog(ZKOperator.class);

	public static ZooKeeper getZookeeperInstance() throws IOException {
		final CountDownLatch cdl = new CountDownLatch(1);
		ZooKeeper zookeeper = new ZooKeeper(ZOOKEEPER_ADDR, RunningConfig.ZK_SESSION_TIMEOUT, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					if (event.getState() == Event.KeeperState.SyncConnected) {
						cdl.countDown();
					}
				}
			});
		return zookeeper;
	}

	public static boolean createZnode(final ZooKeeper zk, String path, String data, final CountDownLatch cdl) throws InterruptedException, KeeperException{
		if(Objects.equal(zk, null)){
			throw new IllegalArgumentException("ERROR:zookeeper instance is not available....");
		}
		if(!Objects.equal(cdl, null)){
			cdl.await();
		}
		StringBuffer buffer=new StringBuffer();
		String[] znodes = path.split("[/]");
		int i;
		String result;
		for(i=0;i<znodes.length-1;i++){
			if(StringUtils.isBlank(znodes[i])){
				continue;
			}
			buffer.append("/").append(znodes[i]);
			if(zk.exists(buffer.toString(), false) != null){
				LOG.info(String.format("znode[%s] has exists,create child...",buffer.toString()));
				continue;
			}else{
				result=zk.create(buffer.toString(), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				LOG.info(String.format("create znode[%s],result[%s]",buffer.toString(),result));
			}
		}
		buffer.append("/").append(znodes[i]);
		if(zk.exists(buffer.toString(), false) == null){
			result = zk.create(buffer.toString(), (data == null ? "" : data).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			LOG.info(String.format("create znode[%s], result[%s]", buffer.toString(), result));
			buffer.setLength(0);
			if(StringUtils.isNotBlank(result)){
				return true;
			}
		}else{
			LOG.info(String.format("znode[%s] has exists",buffer.toString()));
		}
		return false;
	}

	public static void deleteZNode(final ZooKeeper zookeeper, final CountDownLatch cdl, String path) throws InterruptedException, KeeperException{
		if(Objects.equal(zookeeper, null)){
			throw new IllegalArgumentException("ERROR:zookeeper instance is not available....");
		}
		if(!Objects.equal(cdl, null)){
			cdl.await();
		}
		if(zookeeper.exists(path, false)!=null){
			zookeeper.delete(path, -1);
			LOG.info("delete znode:" + path);
		}else{
			LOG.info("znode not exist:" + path);
		}
	}

	public static boolean uploadOperatorInfo(final ZooKeeper zookeeper, final CountDownLatch cdl, String componentName, String mainClass) throws InterruptedException, KeeperException{
		if(Objects.equal(zookeeper, null)){
			throw new IllegalArgumentException("zookeeper instance is not available....,zookeeper=="+zookeeper);
		}
		if(StringUtils.isBlank(componentName)){
			throw new IllegalArgumentException("compenent name is empty....,compenentName=="+componentName);
		}
		if(!Objects.equal(cdl, null)){
			cdl.await();
		}
		String zpath = ZK_COMPONENT_LOCATION + "/" + componentName;
		if(zookeeper.exists(zpath, false) != null){
			LOG.info(String.format("znode:[%s] already exists in zookeeper, when upload operator[%s] info...", zpath, componentName));
			return false;
		}else{
			return ZKOperator.createZnode(zookeeper, zpath, mainClass, cdl);
		}
	}

	public static List<SlavesLocation> getLivingSlaves(ZooKeeper zookeeper) throws KeeperException, InterruptedException{
		LOG.info("Getting living slaves in zookeeper...");
		if(Objects.equal(zookeeper, null)){
			throw new IllegalArgumentException("zookeeper instance is not available....,zookeeper=="+zookeeper);
		}
		List<SlavesLocation> list = new ArrayList<SlavesLocation>();
		List<String> children = zookeeper.getChildren(LIVING_SLAVES, false);
		for(String child : children){
			SlavesLocation loc = new SlavesLocation();
			loc.setIp(child.split(":")[0]);
			loc.setPort(Integer.parseInt(child.split(":")[1]));
			list.add(loc);
		}
		return list;
	}

	public static List<String> getRunningOperators(ZooKeeper zookeeper) throws KeeperException, InterruptedException {
		if(Objects.equal(zookeeper, null)){
			throw new IllegalArgumentException("zookeeper instance is not available....,zookeeper=="+zookeeper);
		}
		return zookeeper.getChildren(SystemConfig.getString("zk_running_operators"), false);
	}

	public static void initTaskResult(final ZooKeeper zookeeper, final CountDownLatch cdl, String taskId, List<TaskResult> results, List<String> ipList, List<Integer> portList) throws InterruptedException, KeeperException{

		if(Objects.equal(zookeeper, null)){
			throw new IllegalArgumentException("zookeeper instance is null...");
		}
		if(Objects.equal(results, null) || results.size()==0){
			throw new IllegalArgumentException("results is null or empty....");
		}

		for (int i = 0; i < ipList.size(); i++) {
			String ip = ipList.get(i);
			String port = portList.get(i) + "";
			String nodeName = ip + ":" + port;
			for (int j = 0; j < results.size(); j++) {
				String operatorMainClass = results.get(j).getMainClass();
				String data = JSON.toJSONString(results.get(j));
				String zPath = TASK_RESULT_PATH + "/" + taskId + "/" + nodeName + "/" + operatorMainClass;
				createZnode(zookeeper, zPath, data, cdl);
				LOG.info(String.format("Task result init, taskId[%s], node[%s], info[%s]", taskId, zPath, data));
			}
		}

	}

	public static void updateTaskResult(final ZooKeeper zookeeper, final CountDownLatch cdl, String taskId, String mainClass, String ip, String port, TaskResult result) throws InterruptedException, KeeperException{

		if(Objects.equal(zookeeper, null)||Objects.equal(result, null)){
			throw new IllegalArgumentException(String.format("zookeeper[%s] or result[%s] is not available...",zookeeper,result));
		}
		if(!Objects.equal(cdl, null)){
			cdl.await();
		}

		String nodeName = ip + ":" + port;
		String zPath = TASK_RESULT_PATH + "/" + taskId + "/" + nodeName + "/" + mainClass;
		if(zookeeper.exists(zPath, false) != null){
			zookeeper.setData(zPath, JSON.toJSONString(result).getBytes(), -1);
			LOG.info(String.format(String.format("Update zk result, taskId:[%s], ZK_NODE_PATH:[%s], RESULT:[%s]", taskId, zPath, JSON.toJSONString(result))));
		}else{
			throw new IllegalArgumentException(String.format("taskId:[%s], ZK_NODE_PATH[%s], is not exists, check taskId, or new .......", taskId, zPath));
		}

	}

	public static void updateValue(final ZooKeeper zookeeper, String zPath, String data, final CountDownLatch cdl) throws InterruptedException, KeeperException{
		if(!Objects.equal(cdl, null)){
			cdl.await();
		}
		if(zookeeper.exists(zPath, false) != null){
			zookeeper.setData(zPath, data.getBytes(), -1);
			LOG.info(String.format(String.format("Update zk result, zPath:[%s], data:[%s] success", zPath, data)));
		}else{
			LOG.error(String.format("zPath:[%s] is not exist", zPath));
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		ZooKeeper zookeeper = ZKOperator.getZookeeperInstance();
		List<String> runningOperators = ZKOperator.getRunningOperators(zookeeper);
		System.out.println(runningOperators.size());
		zookeeper.close();
	}

}
