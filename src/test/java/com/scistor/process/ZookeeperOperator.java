/**
 * 实现指定节点插入、删除、检查等操作
 */
package com.scistor.process;

import com.google.common.base.Objects;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ZookeeperOperator {
    private static CountDownLatch connectedSignal = new CountDownLatch(1);
    private static final Logger LOG= Logger.getLogger(ZookeeperOperator.class);
    private static String hostList = null;
    private static ZooKeeper zkopt = null;
    private static int SESSION_TIMEOUT = 300;
    static{
        try {
            //读取properties文件中的配置连接信息
            InputStream in = ZookeeperOperator.class.getClassLoader().getResourceAsStream("config.properties");
            Properties prop = new Properties();
            prop.load(in);

            //获取地址与端口信息
            hostList = prop.getProperty("zookeeper.urls");
            connect(hostList);
            in.close();
        }catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    public static ZooKeeper getZk(){
        return zkopt;
    }
    /**
     * 连接zookeeper server
     */
    public static void connect(final String hosts) throws Exception {
        try {
            zkopt = new ZooKeeper(hosts, SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        LOG.info("ZooKeeper:" + hosts + " connected... ");
                        connectedSignal.countDown();
                    }
                    LOG.info("event.type == " + event.getType());
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean checkPath(String path){
        try{
            if(zkopt.exists(path,null)==null) {
//                LOG.info("path does not exist " + path);
//                System.out.println("path does not exist!!");
                return false;
            }else{
//                LOG.info("check path done ,path exist " + path);
//                System.out.println("check path done ,path exist!!");
                return true;
            }

        }catch (Exception e ){
//            e.printStackTrace();
            LOG.error(e.toString());
        }
        return false;
    }
    public static void registerSlaveInfo( String path, String taskId) throws Exception {
        String result;
        if(Objects.equal(zkopt, null)){
            LOG.info("ZOOKEEPER IS EXPIRED!");
            System.out.println("ZOOKEEPER IS EXPIRED!");
            connect(hostList);
        }
        if(!Objects.equal(connectedSignal, null)){
            connectedSignal.await();
        }
        if(ZookeeperOperator.checkPath(path)){
            LOG.info(String.format("znode[%s] is exist",path));
            System.out.println("znode is exist , stat update is forbidden" + path);

            zkopt.setData(path, taskId.getBytes(), -1);
            System.out.println("delete old stats and update new stat" + path);
        }else{
            result = zkopt.create(path, taskId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOG.info("upload satus " + result);
        }
        LOG.info("save slave info on zk,info=="+path+" "+taskId);
    }

    public static void delete(String path, String taskId) throws Exception {
        if(Objects.equal(zkopt, null)){
            connect(hostList);
        }
        if(ZookeeperOperator.checkPath(path)){
            //delete any version
            zkopt.delete(path,-1);
            LOG.info("path deleted " + path);
            System.out.println("path deleted"+path);
        }else{
            LOG.info(String.format("znode[%s] does not exists",path));

        }
    }
}
