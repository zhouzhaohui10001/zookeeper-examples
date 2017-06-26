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
    private final CuratorListener curatorListener = new CuratorListener() {
        public void eventReceived(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
            if (curatorEvent
                    .getWatchedEvent()
                    .getType()
                    .compareTo(
                            Watcher.Event.EventType.NodeChildrenChanged) == 0) {
                System.out.println(curatorEvent);
                init();
            }
        }
    };

    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener() {
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            switch (connectionState) {
                case RECONNECTED:
                    System.out.println(connectionState);
                    init();
                    break;
            }
        }
    };
    private List<String> services = null;

    {
        client.start();
        client.getCuratorListenable().addListener(curatorListener);
        client.getConnectionStateListenable().addListener(connectionStateListener);
        init();
    }

    private void init() {
        try {
            List<String> services = client.getChildren().watched().forPath("/" + namespace);
            if (services != null && !services.isEmpty()) {
                this.services = services;
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    public void invoke() {
        if (services == null || services.isEmpty()) {
            throw new RuntimeException("there is no available services");
        }
        Random random = new Random();
        String service = services.get(random.nextInt(services.size()));
        System.out.println("invoke: " + service);
    }

    public static void main(String[] args) throws Throwable {
        Client client = new Client();
        while (true) {
            client.invoke();
            Thread.sleep(3000);
        }
    }
}

