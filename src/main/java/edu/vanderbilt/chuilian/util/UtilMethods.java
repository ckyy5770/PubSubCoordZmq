package edu.vanderbilt.chuilian.util;

import edu.vanderbilt.chuilian.brokers.edge.EdgeBroker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
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
        /*
        InetAddress IP = null;
        try{
            IP = InetAddress.getLocalHost();
        }catch(Exception e){
            logger.error("can not get ip address of this machine. error message: {}", e.getMessage());
        }
        */
        //return IP.toString();
        //return "10.0.0.2";

        String ip = null;
        try{
            ip = readFile("/var/run/hostIP.config");
        }catch(Exception e) {
            logger.error("fail to get ip address of this host.");
        }
        logger.info("Get host ip: " + ip);
        return ip;

    }

    public static String getZookeeperAddress(){
        return "10.0.0.1:2181";
    }

    private static String readFile(String file) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String         line = null;
        StringBuilder  stringBuilder = new StringBuilder();
        String         ls = System.getProperty("line.separator");

        try {
            while((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append(ls);
            }

            return stringBuilder.toString();
        } finally {
            reader.close();
        }
    }
}
