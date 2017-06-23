package cn.timd.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public abstract class BaseConfiguration {
    public static final String connectString =
        "192.168.30.2:2181,192.168.30.3:2181,192.168.30.4:2181";
    public static final ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, Integer.MAX_VALUE);
    public static final int sessionTimeoutMS = 10000;
    public static final int connectionTimeoutMS = 3000;

    public final CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(connectString)
            .retryPolicy(retryPolicy)
            .sessionTimeoutMs(sessionTimeoutMS)
            .connectionTimeoutMs(connectionTimeoutMS)
            .build();
}
