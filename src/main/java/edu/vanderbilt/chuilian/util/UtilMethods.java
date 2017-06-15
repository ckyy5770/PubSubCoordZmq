package edu.vanderbilt.chuilian.util;

import edu.vanderbilt.chuilian.brokers.edge.EdgeBroker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;

/**
 * Created by Killian on 6/15/17.
 */


public class UtilMethods {
    private static final Logger logger = LogManager.getLogger(UtilMethods.class.getName());


    private UtilMethods(){}

    /**
     * get ip address of this machine
     * @return
     * @throws Exception
     */
    public static String getIPaddress(){
        InetAddress IP = null;
        try{
            IP = InetAddress.getLocalHost();
        }catch(Exception e){
            logger.error("can not get ip address of this machine. error message: {}", e.getMessage());
        }
        //return IP.toString();
        return "127.0.0.1";
    }

    public static String getZookeeperAddress(){
        return "127.0.0.1:2181";
    }
}
