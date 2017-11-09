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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by Administrator on 2017/11/8.
 */
public class ComponentRegisterTest {

	private static final Log LOG = LogFactory.getLog(ClientTest.class);
	private static final int SESSION_TIMEOUT=30000;

	public static void main(String[] args) throws TException, IOException {
		String SERVER_IP="127.0.0.1";
		int SERVER_PORT=18081;

		FileInputStream fis = new FileInputStream(new File("F:\\ClassloadTest\\ClassLoad.jar"));
		byte[] b=new byte[fis.available()];
		fis.read(b);
		fis.close();
		ByteBuffer buffer= ByteBuffer.wrap(b);

		TTransport trans=new TSocket(SERVER_IP,SERVER_PORT,SESSION_TIMEOUT);
		TFramedTransport transport=new TFramedTransport(trans);
		TProtocol protocol=new TCompactProtocol(transport);
		MasterService.Client client=new MasterService.Client(protocol);
		transport.open();
		String result = client.registerComponent("ClassLoad", "mainclass1", buffer);
		transport.close();
	}

}
