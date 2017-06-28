package cn.timd.zookeeper.NamingService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.Watcher;

import java.util.List;
import java.util.Random;

public class Client extends BaseNamingService {
    private List<String> providers = null;
    private final Object condition = new Object();

    private final CuratorListener curatorListener = new CuratorListener() {
        public void eventReceived(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
            if (curatorEvent
                    .getWatchedEvent()
                    .getType()
                    .compareTo(
                            Watcher.Event.EventType.NodeChildrenChanged) == 0) {
                System.out.println(curatorEvent);
                synchronized (condition) {
                    getProviders();
                }
            }
        }
    };

    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener() {
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            System.out.println(connectionState);
            switch (connectionState) {
                case CONNECTED:
                case RECONNECTED:
                    synchronized (condition) {
                        getProviders();
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

    private void getProviders() {
        try {
            List<String> providers = client.getChildren().watched().forPath("/" + namespace);
            if (providers != null && !providers.isEmpty()) {
                this.providers = providers;
                condition.notifyAll();
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    public void invoke() {
        if (providers == null || providers.isEmpty()) {
            throw new RuntimeException("there is no available providers");
        }
        Random random = new Random();
        String service = providers.get(random.nextInt(providers.size()));
        System.out.println("invoke: " + service);
    }

    public static void main(String[] args) throws Throwable {
        Client client = new Client();
        synchronized (client.condition) {
            if (client.providers == null || client.providers.isEmpty())
                client.condition.wait();
        }
        for (int i = 0; i <= 1000; i++) {
            client.invoke();
            Thread.sleep(2000);
        }
    }
}

