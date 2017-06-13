package edu.vanderbilt.chuilian.brokers.edge;

import edu.vanderbilt.chuilian.loadbalancer.Dispatcher;
import edu.vanderbilt.chuilian.loadbalancer.LoadAnalyzer;
import edu.vanderbilt.chuilian.util.PortList;
import edu.vanderbilt.chuilian.util.ZkConnect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EdgeBroker {
    private String brokerID;
    private ChannelMap channelMap;
    private final PortList portList;
    // the executor for channel threads
    private final ExecutorService channelExecutor;
    private final ZkConnect zkConnect;
    // load balancer module: dispatcher, local load analyzer
    private final Dispatcher dispatcher;
    private final LoadAnalyzer loadAnalyzer;

    private static final Logger logger = LogManager.getLogger(EdgeBroker.class.getName());

    /**
     * Constructor will NOT immediately create socket and bind any port,
     * It only pass in some critical information for constructing the connection,
     * to start the edgeBroker, use start().
     */
    public EdgeBroker(){
        // get broker ID
        this.brokerID = getBrokerID();
        // init channel map
        this.channelMap = new ChannelMap();
        // init port list
        this.portList = new PortList();
        // init executors
        this.channelExecutor = Executors.newFixedThreadPool(20);
        // make a new zookeeper connector
        this.zkConnect = new ZkConnect();
        // load balancer module: create a dispatcher
        this.dispatcher = new Dispatcher(brokerID, zkConnect, channelMap);
        this.loadAnalyzer = new LoadAnalyzer(brokerID, zkConnect);
    }

    /**
     * start the broker
     */
    public void start() throws Exception {
        // start zookeeper client
        zkConnect.connect("127.0.0.1:2181");
        // clear the data tree
        // zkConnect.resetServer();
        // load balancer module: start dispatcher
        dispatcher.start();
        loadAnalyzer.start();
        // create and start main channel
        MainChannel mainChannel = new MainChannel("", this.portList, this.channelExecutor, this.zkConnect, this.channelMap, this.dispatcher, this.loadAnalyzer);
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
        // close dispatcher and local load analyzer
        dispatcher.stop();
        loadAnalyzer.stop();
        // clear the data tree
        //zkConnect.resetServer();
        // turn off executor
        channelExecutor.shutdownNow();
        // close zookeeper client
        zkConnect.close();
    }

    /**
     * get broker ID, here just a fake method for test purpose
     *
     * @return
     */
    private String getBrokerID() {
        return Long.toString(System.currentTimeMillis());
    }


    public static void main(String args[]) throws Exception {
        EdgeBroker broker = new EdgeBroker();
        broker.start();
        //Thread.sleep(80000);
        //broker.stop();
    }


}
