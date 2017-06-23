package cn.timd.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;

public class ConfigurationManagement extends BaseConfiguration {
    private static final String namespace = "service/common-configuration";
    private static final String nodeName = "test";
    private static final String charSet = "utf8";
    private volatile String configuration;

    private final CuratorListener listener = new CuratorListener() {
        public void eventReceived(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
            updateConfiguration(curatorFramework, curatorEvent);
        }
    };

    {
        client.start();
        try {
            configuration = new String(
                    client.usingNamespace(namespace)
                            .getData()
                            .watched()
                            .forPath(nodeName),
                    charSet);
            client.getCuratorListenable().addListener(listener);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex.getMessage());
        }
    }

    private void updateConfiguration(CuratorFramework curatorFramework, CuratorEvent curatorEvent) {
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

    public void work() {
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

