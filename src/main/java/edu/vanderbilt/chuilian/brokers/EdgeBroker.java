package edu.vanderbilt.chuilian.brokers;

import edu.vanderbilt.chuilian.util.ChannelMap;
import edu.vanderbilt.chuilian.util.MainChannel;
import edu.vanderbilt.chuilian.util.PortList;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EdgeBroker {
    private ChannelMap channelMap;
    private PortList portList;
    // the executor for working threads
    private ExecutorService executor;

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
        this.executor = Executors.newFixedThreadPool(100);
    }

    /**
     * start the broker
     */
    public void start(){
        // create and start main channel
        MainChannel mainChannel = new MainChannel("", this.portList, this.executor, this.channelMap);
        mainChannel.start();
    }

    public static void main(String args[]){
        EdgeBroker broker = new EdgeBroker();
        broker.start();
    }


}
