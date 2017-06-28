package cn.timd.zookeeper.NamingService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;

public class Service extends BaseNamingService {
    private final String url = "127.0.0.1:8887";

    private final ConnectionStateListener listener = new ConnectionStateListener() {
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            System.out.println(connectionState);
            System.out.println(Thread.currentThread().getName());
            switch (connectionState) {
                case RECONNECTED:
                    unregister();
                case CONNECTED:
                    register();
                    break;
            }
        }
    };

    {
        client.start();
        client.getConnectionStateListenable().addListener(listener);
    }

    private void register() {
        try {
            client.usingNamespace(namespace).create().withMode(CreateMode.EPHEMERAL).forPath(url);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    private void unregister() {
        try {
            client.usingNamespace(namespace).delete().forPath("/" + url);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void work() throws InterruptedException {
        while (true) {
            System.out.println("I am working");
            Thread.sleep(3000);
        }
    }

    public static void main(String[] args) {
        Service service = null;
        try {
            (service = new Service()).work();
        } catch (InterruptedException ex) {
            if (service != null)
                service.unregister();
        }
    }
}

