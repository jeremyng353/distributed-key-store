package com.g2.CPEN431.A11;

import org.apache.zookeeper.*;

import java.io.*;
import java.nio.charset.StandardCharsets;

/*
- so each node will have a zookeeperclient to create a znode for themselves
- the zookeeper server sends events to clients that have registered a watcher object on a znode or parent znode
- watcher objects can choose what kind of events to listen for, and if a watcher object is registered with
  the server, then the server will send a notification to the client indicating the event has occurred

 */

public class ZooKeeperClient implements Watcher {

    private ZooKeeper zooKeeper;
    private String zNodePath;
    private int port;
    private boolean internalData = true;

    public ZooKeeperClient(int port) {
        String zkUrl = "localhost:2181"; // The URL of your ZooKeeper cluster
        int sessionTimeout = 5000; // The ZooKeeper session timeout
        try {
            // connect to zookeeper server, this object represents the server
            this.zooKeeper = new ZooKeeper(zkUrl, sessionTimeout, this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.port = port;

        // TODO: create a znode
        try {
            registerNode(String.valueOf(this.port));
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }


    /*
    1. node that wants to put/get/remove a key: check if znode exists
        1a. doesn't exist: create znode
        1b. does exist
            1b1. add watcher to znode
            1b2. upon death of znode, run the command in the packet
     */

    @Override
    public void process(WatchedEvent event) {
        // TODO: Handle ZooKeeper events
        if (event.getType() == Event.EventType.NodeDataChanged && event.getPath().equals(zNodePath)) {
            // The data for the zNode we're watching has changed, so we need to handle it here
            try {
                byte[] data = zooKeeper.getData(zNodePath, this, null);
                String newData = new String(data, StandardCharsets.UTF_8);
                System.out.println("Data for zNode " + zNodePath + " has changed to: " + newData);
            } catch (KeeperException | InterruptedException e) {
                // Handle any errors that may occur while getting the zNode data
                e.printStackTrace();
            }
        }
    }

    public void editData() {
        try {
            // retrieves znode since watch is false
            int version = zooKeeper.exists(zNodePath, false).getVersion();
            zooKeeper.setData(zNodePath, null, version);
        } catch (KeeperException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void editData(String key) {
        String zNodePath = "/locks/" + key;
        try {
            // Try to create the lock znode with EPHEMERAL flag
            String lockPath = zooKeeper.create(zNodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            // If the creation succeeds, the lock has been obtained, so proceed with the operation
            int version = zooKeeper.exists(zNodePath, false).getVersion();
            zooKeeper.setData(zNodePath, null, version);

            // Release the lock by deleting the lock znode
            zooKeeper.delete(lockPath, -1);
        } catch (KeeperException.NodeExistsException e) {
            // If the creation fails due to NodeExistsException, it means the lock is held by another node
            // so retry later
            editData(key);
        } catch (KeeperException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void registerNode(String nodeName) throws KeeperException, InterruptedException {
        // Create a ZNode for the server node
        zNodePath = "/server-nodes/" + nodeName;
        CreateMode mode = CreateMode.PERSISTENT;
        this.zooKeeper.create(zNodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
    }

    public void registerAllWatchers() {
        // TODO: pass in every possible path (port range)
        for (int i = 4445; i < 4485; i++) {
            try {
                // registers znode since watch is true
                this.zooKeeper.exists("/server-nodes/" + i, true);
            } catch (KeeperException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void close() throws InterruptedException {
        // Close the ZooKeeper connection
        this.zooKeeper.close();
    }
}
