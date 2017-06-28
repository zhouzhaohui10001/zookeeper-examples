package cn.timd.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.Watcher;

public class ConfigurationManagement extends BaseConfiguration {
    private static final String namespace = "service/common-configuration";
    private static final String nodeName = "test";
    private static final String charSet = "utf8";
    private final Object condition = new Object();
    private volatile String configuration;

    private final CuratorListener curatorListener= new CuratorListener() {
        public void eventReceived(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
            if (curatorEvent.getWatchedEvent().getType().compareTo(
                    Watcher.Event.EventType.NodeDataChanged) == 0) {
                System.out.println(curatorEvent);
                System.out.println(Thread.currentThread().getName());
                synchronized (condition) {
                    updateConfiguration();
                    condition.notifyAll();
                }
            }
        }
    };

    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener() {
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            System.out.println(connectionState);
            System.out.println(Thread.currentThread().getName());
            switch (connectionState) {
                case CONNECTED:
                case RECONNECTED:
                    synchronized (condition) {
                        updateConfiguration();
                        condition.notifyAll();
                    }
                    break;
            }
        }
    };

    {
        client.start();
        client.getConnectionStateListenable().addListener(connectionStateListener);
        client.getCuratorListenable().addListener(curatorListener);
    }

    private void updateConfiguration() {
        System.out.println("update configuration");
        try {
            configuration = new String(
                    client.usingNamespace(namespace)
                            .getData()
                            .watched()
                            .forPath(nodeName),
                    charSet);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex.getMessage());
        }
    }

    public void work() throws Exception {
        synchronized (condition) {
            if (configuration == null)
                condition.wait();
        }

        while (true) {
            System.out.println("Configuration is: " + configuration);
            Thread.sleep(2000);
        }
    }

    public static void main(String[] args) throws Throwable {
        System.out.println(Thread.currentThread().getName());
        new ConfigurationManagement().work();
    }
}

