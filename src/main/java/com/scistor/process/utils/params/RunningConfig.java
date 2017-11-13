package com.scistor.process.utils.params;

public interface RunningConfig {

	/**
	 * spark|ms 系统任务并发度
	 */
	Integer TASK_CONCURRENT_NUM = Integer.parseInt(SystemConfig.getString("concurrent.num"));
	Integer TASK_REQUEST_MAX = Integer.parseInt(SystemConfig.getString("request.max"));

	/**
	 * 存活的从节点 thrift 服务ip,端口信息
	 */
	String LIVING_SLAVES = "/HS/slave";

	String COMPONENT_LOCATION = SystemConfig.getString("compenent_location");
	String ZOOKEEPER_ADDR = SystemConfig.getString("zookeeper_addr");
	String ZK_COMPONENT_LOCATION = SystemConfig.getString("zk_compenent_location");

	Integer MASTER_SELECTOR_THREADS = Integer.parseInt(SystemConfig.getString("master_selector_threads"));
	Integer MASTER_THREAD_POOL_SIZE = Integer.parseInt(SystemConfig.getString("master_thread_pool_size"));

	Integer ZK_SESSION_TIMEOUT = 24*60*60*1000; //24hour
}
