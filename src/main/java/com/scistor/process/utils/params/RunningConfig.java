package com.scistor.process.utils.params;

public interface RunningConfig {

	/**
	 * 存活的从节点 thrift 服务ip,端口信息
	 */
	String LIVING_SLAVES = "/HS/slave";
	String TASK_RESULT_PATH = "/HS/tasks";

	String COMPONENT_LOCATION = SystemConfig.getString("compenent_location");
	String ZOOKEEPER_ADDR = SystemConfig.getString("zookeeper_addr");
	String ZK_COMPONENT_LOCATION = SystemConfig.getString("zk_compenent_location");
	String ZK_RUNNING_OPERATORS = SystemConfig.getString("zk_running_operators");

	Integer MASTER_SELECTOR_THREADS = Integer.parseInt(SystemConfig.getString("master_selector_threads"));
	Integer MASTER_THREAD_POOL_SIZE = Integer.parseInt(SystemConfig.getString("master_thread_pool_size"));

	Integer ZK_SESSION_TIMEOUT = 24*60*60*1000; //24hour

	Integer THRIFT_SESSION_TIMEOUT = 30000;
}
