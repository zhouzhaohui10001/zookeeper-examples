package cn.timd.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;
import java.util.List;

public class ReadWriteLock extends BaseConfiguration {
    private String namespace;
    private String prefix;
    private String nodeName;
    private Byte magic;
    private final Object condition = new Object();

    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener() {
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            System.out.println(connectionState);
            switch (connectionState) {
                case SUSPENDED:
                case LOST:
                    synchronized (condition) {
                        condition.notifyAll();
                    }
                    break;
                case RECONNECTED:
                    try {
                        if (nodeName != null)
                            client.usingNamespace(namespace).delete().forPath(nodeName);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }

                    try {
                        nodeName = createNode();
                        synchronized (condition) {
                            condition.notifyAll();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
            }
        }
    };

    private final CuratorListener curatorListener = new CuratorListener() {
        public void eventReceived(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
            if (curatorEvent.getWatchedEvent().getType().compareTo(Watcher.Event.EventType.NodeDeleted) == 0) {
                if (nodeName != null && magic != null)
                    synchronized (condition) {
                        condition.notifyAll();
                    }
            }
        }
    };

    {
        client.start();
        client.getCuratorListenable().addListener(curatorListener);
    }

    public ReadWriteLock() {
        this("lock/test", "read-write-lock-");
    }

    public ReadWriteLock(String namespace, String prefix) {
        this.namespace = namespace;
        this.prefix = prefix;
    }

    private boolean isAcquired() throws Exception {
        List<String> children = client.getChildren().forPath("/" + namespace);
        String[] childArray = new String[children.size()];
        for (int i = 0; i < children.size(); ++i)
            childArray[i] = children.get(i);
        Arrays.sort(childArray);
        if (childArray[0].equals(nodeName))
            return true;

        String previousNodeName = null;
        String previousWriteNodeName = null;
        for (int i = 0; i < childArray.length - 1; ++i) {
            if (childArray[i].equals(nodeName))
                break;
            if (childArray[i + 1].equals(nodeName))
                previousNodeName = childArray[i];

            byte[] bytes = client.usingNamespace(namespace).getData().forPath(childArray[i]);
            if (bytes[0] == 1)
                previousWriteNodeName = childArray[i];
        }

        if (magic == 0 && previousWriteNodeName == null)
            return true;
        else if (magic == 0) {
            Stat stat = client.usingNamespace(namespace).checkExists().watched().forPath(previousWriteNodeName);
            if (stat == null)
                return true;
            return false;
        }
        else {
            Stat stat = client.usingNamespace(namespace).checkExists().watched().forPath(previousNodeName);
            if (stat == null)
                isAcquired();
            return false;
        }
    }

    private String createNode() throws Exception {
        return client.usingNamespace(namespace)
                .create()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(prefix, new byte[]{magic}).substring(1);
    }

    private boolean acquireLock(String nodeName, byte magic) throws Exception {
        this.magic = magic;
        if (nodeName == null)
            this.nodeName = nodeName = createNode();

        boolean result;
        while (!(result = isAcquired())) {
            synchronized (condition) {
                condition.wait();
            }
        }
        return result;
    }

    public boolean acquireReadLock() throws Exception {
        return acquireLock(null, (byte)0);
    }

    public boolean acquireWriteLock() throws Exception {
        return acquireLock(null, (byte)1);
    }

    public void releaseLock() throws Exception {
        if (nodeName != null) {
            client.usingNamespace(namespace).delete().forPath(nodeName);
            nodeName = null;
            magic = null;
        }
    }

    public static void main(String[] args) throws Exception {
        ReadWriteLock lock = new ReadWriteLock();
        lock.acquireWriteLock();
        System.out.println("acquire lock");
        lock.acquireWriteLock();
        System.out.println("acquire lock");
        lock.releaseLock();
    }
}

