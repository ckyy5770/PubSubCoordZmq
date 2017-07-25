package edu.vanderbilt.chuilian.test.reconfiguration;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

/**
 * Created by Killian on 7/25/17.
 */
public class TestSimple implements Runnable {
    private ZMQ.Context context;
    private ZMQ.Socket socket;
    private boolean stop = false;
    public TestSimple(){
        context = ZMQ.context(1);
        socket = context.socket(ZMQ.SUB);
    }

    @Override
    public void run() {
        socket.bind("tcp://127.0.0.1:5000");
        socket.subscribe("".getBytes());
        try{
            while(!stop){
                ZMsg msg = ZMsg.recvMsg(socket);
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally{
            socket.close();
        }
    }

    public void stop(){
        stop = true;
        socket.close();
        //context.term();
    }

    public static void main(String[] args) throws InterruptedException {
        TestSimple ts = new TestSimple();
        Thread t1 = new Thread(ts);
        t1.start();
        Thread.sleep(2000);
        ts.stop();
        System.out.println("finished");
    }

}
