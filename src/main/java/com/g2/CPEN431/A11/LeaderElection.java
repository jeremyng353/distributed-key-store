package com.g2.CPEN431.A11;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;


// the clients (key-value servers) elect the leader (one of the ZooKeeper servers)
public class LeaderElection implements Watcher {
    private final ZooKeeper zooKeeper;
    private final String parentNode;
    private String currentZnodeName;
    private final CountDownLatch latch = new CountDownLatch(1);

    public LeaderElection(ZooKeeper zooKeeper, String parentNode) {
        this.zooKeeper = zooKeeper;
        this.parentNode = parentNode;
    }

    public void start() throws InterruptedException, KeeperException {
        createParentNodeIfNeeded();
        joinElection();
        latch.await();
    }

    public void stop() throws InterruptedException, KeeperException {
        zooKeeper.close();
    }

    private void createParentNodeIfNeeded() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(parentNode, false);
        if (stat == null) {
            zooKeeper.create(parentNode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    private void joinElection() throws KeeperException, InterruptedException {
        String znodePrefix = "n_";
        String znodeFullPath = zooKeeper.create(parentNode + "/" + znodePrefix, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        currentZnodeName = znodeFullPath.replace(parentNode + "/", "");
        watchPreviousZnode();
    }

    private void watchPreviousZnode() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(parentNode, false);
        Collections.sort(children);
        int currentIndex = children.indexOf(currentZnodeName);
        if (currentIndex == 0) {
            becomeLeader();
        } else {
            String previousZnodeName = children.get(currentIndex - 1);
            Stat stat = zooKeeper.exists(parentNode + "/" + previousZnodeName, this);
            if (stat == null) {
                watchPreviousZnode();
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case NodeDeleted:
                try {
                    watchPreviousZnode();
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            case None:
                if (event.getState() == Event.KeeperState.Expired) {
                    latch.countDown();
                }
                break;
            default:
                break;
        }
    }

    private void becomeLeader() throws KeeperException, InterruptedException {
        zooKeeper.setData(parentNode, currentZnodeName.getBytes(), -1);
        System.out.println("I'm the leader");
        latch.countDown();
    }
}
