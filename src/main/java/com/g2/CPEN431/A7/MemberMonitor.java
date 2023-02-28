package com.g2.CPEN431.A7;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

public class MemberMonitor implements Runnable{

    // A HashMap to store node information
    HashMap<InetAddress, Pair<LocalDateTime, Boolean>> nodeStore;

    //dummy time until we set the amount of nodes
    final int FULL_INFECTION_TIME = 10000;

    @Override
    public run() {
        TimerTask pullEpidemic = new TimerTask() {
            @Override
            public void run() {
                // grab a random node from the nodeStore at random, and pull its status
                // update the nodeStore if the value received is older than what is saved
                // if no value is returned past the timeout, then check if the node is past the FULL_INFECTION_TIME limit
                    // if so, nodeStore.get(randomNode.InetAddress).setBoolean(false)
            }
        };

        Timer timer = new Timer("Send Timer");
        long delay =
        timer.schedule(pullEpidemic, delay);

        while (true) {
            //listen for other nodes
        }

    }
}
