package edu.vanderbilt.chuilian.util;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Killian on 5/23/17.
 */

/**
 * This object maintains an available port list for newly created channel
 * Since each channel has to create its own sockets and use its own ports to receive and send messages
 * It needs to use PortList to require a new port.
 */
public class PortList {
    private Queue<Integer> list;
    public PortList(){
        this.list = new LinkedList<>();
        // add some port number as available ports
        for(int i=5000; i<25000; i++) this.list.add(i);
    }

    /**
     * get an avaible port number
     * @return null if no ports avaible
     */
    public int get(){
        return list.poll();
    }

    /**
     * return unused port back to the port list
     */
    public void put(int portNum){
        list.add(portNum);
    }


}
