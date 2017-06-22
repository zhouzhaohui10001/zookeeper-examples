package cn.timd.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;

public class ConfigurationManagement extends BaseConfiguration {
    private final CuratorFramework client;
    private volatile String configuration;
    private final String path = "/service/common-configuration";
    private String charSet = "utf8";
    private final CuratorListener listener = new CuratorListener() {
        public void eventReceived(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
            updateConfiguration(curatorFramework, curatorEvent);
        }
    };

    {
        client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(sessionTimeoutMS)
                .connectionTimeoutMs(connectionTimeoutMS)
                .build();
        client.getCuratorListenable().addListener(listener);
        client.start();

        try {
            configuration = new String(client.getData().watched().forPath(path), charSet);
        } catch (Exception ex) {
            ex.printStackTrace();
            configuration = null;
        }
    }

    private void updateConfiguration(CuratorFramework curatorFramework, CuratorEvent curatorEvent) {
        try {
            configuration = new String(client.getData().watched().forPath(path), charSet);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void work() {
        if (configuration == null)
            throw new RuntimeException("configuration is null");

        while (true) {
            System.out.println("Configuration is: " + configuration);

            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                break;
            }
        }
    }

    public static void main(String[] args) {
        new ConfigurationManagement().work();
    }
}

