package com.scistor.process.thrift.client;

import com.scistor.process.thrift.service.MasterService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by Administrator on 2017/11/6.
 */
public class ClientTest {

	private static final Log LOG = LogFactory.getLog(ClientTest.class);
	private static final int SESSION_TIMEOUT=30000;

	//测试程序入口
	public static void main(String[] args) throws TException, ClassNotFoundException, IOException {
		String xmlContent="";
		FileInputStream fis =new FileInputStream("D:\\x.txt");
		byte[] b=new byte[fis.available()];
		fis.read(b);
		fis.close();
		xmlContent=new String(b,"utf-8");
		String SERVER_IP="127.0.0.1";
		int SERVER_PORT=18081;
		for(int i=0;i<1;i++) {
			String taskId = "task1";
			TTransport trans=new TSocket(SERVER_IP,SERVER_PORT,SESSION_TIMEOUT);
			TFramedTransport transport=new TFramedTransport(trans);
			TProtocol protocol=new TCompactProtocol(transport);
			MasterService.Client client=new MasterService.Client(protocol);
			transport.open();
			String result=client.newTask(taskId, xmlContent);
			System.out.println(String.format("Result is [%s]", result));
			LOG.error(String.format("taskId=%s,result=%s",taskId,result));
			transport.close();
			LOG.error("*********************************");
			LOG.error("第"+i+"次连接-提交任务-断开连接 完成");
			LOG.error("#################################");
		}
	}

}
