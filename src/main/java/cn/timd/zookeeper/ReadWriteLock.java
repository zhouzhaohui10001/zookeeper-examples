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
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ReadWriteLock extends BaseConfiguration {
    private String namespace;
    private String prefix;
    private String nodeName;
    private Byte magic;
    private final Object condition = new Object();

    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener() {
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            switch (connectionState) {
                case SUSPENDED:
                case LOST:
                    synchronized (condition) {
                        System.out.println(connectionState);
                        condition.notifyAll();
                    }
                    break;
                case RECONNECTED:
                    synchronized (condition) {
                        System.out.println(connectionState);
                        if (nodeName == null)
                            break;
                        try {
                            client.usingNamespace(namespace).delete().forPath(nodeName);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }

                        try {
                            nodeName = createNode();
                            condition.notifyAll();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                    break;
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
        client.getConnectionStateListenable().addListener(connectionStateListener);
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
            this.nodeName = createNode();

        boolean result;
        while (!(result = isAcquired())) {
            condition.wait();
        }
        return result;
    }

    public boolean acquireReadLock() throws Exception {
        if (nodeName != null && magic != null)
            throw new RuntimeException("illegal state");
        synchronized (condition) {
            return acquireLock(null, (byte) 0);
        }
    }

    public boolean acquireWriteLock() throws Exception {
        if (nodeName != null && magic != null)
            throw new RuntimeException("illegal state");
        synchronized (condition) {
            return acquireLock(null, (byte) 1);
        }
    }

    public void releaseLock() throws Exception {
        if (nodeName != null && magic != null) {
            client.usingNamespace(namespace).delete().forPath(nodeName);
            nodeName = null;
            magic = null;
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(10);

        Runnable readRunnable = new Runnable() {
            public void run() {
                try {
                    ReadWriteLock readWriteLock = new ReadWriteLock();
                    String threadName = Thread.currentThread().getName();
                    System.out.println(String.format(threadName + " begins to acquire read lock @%tr", new Date()));
                    readWriteLock.acquireReadLock();
                    System.out.println(String.format(threadName + " acquired read lock @%tr", new Date()));
                    Thread.sleep((long)(10000 * Math.random()));
                    readWriteLock.releaseLock();
                    System.out.println(String.format(threadName + " release read lock @%tr", new Date()));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };

        Runnable writeRunnable = new Runnable() {
            public void run() {
                try {
                    ReadWriteLock readWriteLock = new ReadWriteLock();
                    String threadName = Thread.currentThread().getName();
                    System.out.println(String.format(threadName + " begins to acquire write lock @%tr", new Date()));
                    readWriteLock.acquireWriteLock();
                    System.out.println(String.format(threadName + " acquired write lock @%tr", new Date()));
                    Thread.sleep((long)(10000 * Math.random()));
                    readWriteLock.releaseLock();
                    System.out.println(String.format(threadName + " release write lock @%tr", new Date()));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };

        for (int i = 0; i < 10; i++) {
            service.submit(i % 2 == 0 ? readRunnable : writeRunnable);
            Thread.sleep(1000);
        }

        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);
    }
}

