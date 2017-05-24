package edu.vanderbilt.chuilian.util;

/**
 * Created by Killian on 5/24/17.
 */
public class DefaultSender extends DataSender {
    public DefaultSender(String address) {
        super("", address);
    }

    public void send(String topic, String message) {
        this.sendSocket.sendMore(topic);
        this.sendSocket.send(message);
    }

    public void send(String message) {
        throw new UnsupportedOperationException("you must specify message topic when sending message through default data sender!");
    }

    public static void main(String[] args) {
    }

}
