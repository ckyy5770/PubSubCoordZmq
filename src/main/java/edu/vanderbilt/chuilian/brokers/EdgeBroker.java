package edu.vanderbilt.chuilian.brokers;

import edu.vanderbilt.chuilian.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EdgeBroker {
    private ChannelMap channelMap;
    private final PortList portList;
    // the executor for channel threads
    private final ExecutorService channelExecutor;
    private final ZkConnect zkConnect;
    private static final Logger logger = LogManager.getLogger(EdgeBroker.class.getName());

    /**
     * Constructor will NOT immediately create socket and bind any port,
     * It only pass in some critical information for constructing the connection,
     * to start the edgeBroker, use start().
     */
    public EdgeBroker(){
        // init channel map
        this.channelMap = new ChannelMap();
        // init port list
        this.portList = new PortList();
        // init executors
        this.channelExecutor = Executors.newFixedThreadPool(10);
        // make a new zookeeper connector
        this.zkConnect = new ZkConnect();
    }

    /**
     * start the broker
     */
    public void start() throws Exception {
        // start zookeeper client
        zkConnect.connect("127.0.0.1:2181");
        // clear the data tree
        zkConnect.resetServer();
        // create and start main channel
        MainChannel mainChannel = new MainChannel("", this.portList, this.channelExecutor, this.zkConnect, this.channelMap);
        channelMap.setMain(mainChannel);
        mainChannel.start();
    }

    /**
     * stop the broker
     *
     * @throws Exception
     */
    public void stop() throws Exception {
        // close every message channel, including default channel
        // stop default channel first
        channelMap.getMain().stop();
        // close any other channel
        // iterate through the map, shutdown every single sender.
        for (Map.Entry<String, MsgChannel> entry : channelMap.entrySet()) {
            entry.getValue().stop();
        }
        // create a new map, discard the old one
        channelMap = new ChannelMap();
        // clear the data tree
        zkConnect.resetServer();
        // turn off executor
        channelExecutor.shutdownNow();
        // close zookeeper client
        zkConnect.close();
    }


    public static void main(String args[]) throws Exception {
        EdgeBroker broker = new EdgeBroker();
        broker.start();
        Thread.sleep(60000);
        broker.stop();
    }


}
