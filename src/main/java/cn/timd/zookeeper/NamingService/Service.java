package cn.timd.zookeeper.NamingService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;

public class Service extends BaseNamingService {
    private final String url = "127.0.0.1:8889";

    private final ConnectionStateListener listener = new ConnectionStateListener() {
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            System.out.println(connectionState);

            switch (connectionState) {
                case RECONNECTED:
                    try {
                        client.usingNamespace(namespace).delete().forPath(url);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    init();
                    break;
            }
        }
    };

    {
        client.start();
        client.getConnectionStateListenable().addListener(listener);
        init();
    }

    private void init() {
        try {
            client.usingNamespace(namespace).create().withMode(CreateMode.EPHEMERAL).forPath(url);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    private void work() {
        while (true) {
            System.out.println("I am working");

            try {
                Thread.sleep(3000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                break;
            }
        }
    }

    public static void main(String[] args) {
        new Service().work();
    }
}

