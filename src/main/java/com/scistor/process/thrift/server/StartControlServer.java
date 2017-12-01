package com.scistor.process.thrift.server;

import com.scistor.process.thrift.service.MasterService;
import com.scistor.process.thrift.service.MasterServiceImpl;
import com.scistor.process.utils.params.RunningConfig;
import com.scistor.process.utils.params.SystemConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2017/11/6.
 */
public class StartControlServer {

	private static final Log LOG = LogFactory.getLog(StartControlServer.class);

	public static void main(String[] args) throws NumberFormatException, TTransportException {
		start();
	}

	public static void start() throws NumberFormatException, TTransportException{

		TProcessor processor = new MasterService.Processor<MasterService.Iface>(new MasterServiceImpl());
		LOG.info("thrift processor init finished...");
		//transport
		TNonblockingServerSocket serverSocket = new TNonblockingServerSocket(Integer.parseInt(SystemConfig.getString("thrift_server_port")));
		LOG.info("thrift transport init finished...");

		TThreadedSelectorServer.Args m_args = new TThreadedSelectorServer.Args(serverSocket);
		m_args.processor(processor);
		m_args.processorFactory(new TProcessorFactory(processor));
		m_args.protocolFactory(new TCompactProtocol.Factory());
		m_args.transportFactory(new TFramedTransport.Factory(16384000*30));
		m_args.selectorThreads(RunningConfig.MASTER_SELECTOR_THREADS);

		ExecutorService threads= Executors.newFixedThreadPool(RunningConfig.MASTER_THREAD_POOL_SIZE);
		m_args.executorService(threads);
		LOG.info("thrift nio channel selector  init finished...");
		TThreadedSelectorServer server=new TThreadedSelectorServer(m_args);
		LOG.info("server starting...");
		server.serve();
	}

}
