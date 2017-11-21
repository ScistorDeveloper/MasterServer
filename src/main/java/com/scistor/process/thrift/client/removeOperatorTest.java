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

import java.util.ArrayList;
import java.util.List;

/**
 * Created by WANG Shenghua on 2017/11/20.
 */
public class removeOperatorTest {

    private static final Log LOG = LogFactory.getLog(removeOperatorTest.class);
    private static final int SESSION_TIMEOUT=30000;

    public static void main(String[] args) throws TException {

        String SERVER_IP="127.0.0.1";
        int SERVER_PORT=18081;
        TTransport trans=new TSocket(SERVER_IP,SERVER_PORT,SESSION_TIMEOUT);
        TFramedTransport transport=new TFramedTransport(trans);
        TProtocol protocol=new TCompactProtocol(transport);
        MasterService.Client client=new MasterService.Client(protocol);
        transport.open();
        List<String> mainClassList = new ArrayList<String>();
        mainClassList.add("com.scistor.process.operator.impl.WhiteListFilterOperator");
        String result = client.removeOperators(mainClassList);
        System.out.println(String.format("Result is [%s]", result));
        transport.close();

    }

}
