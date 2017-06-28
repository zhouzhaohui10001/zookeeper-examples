package cn.timd.zookeeper;

import org.apache.curator.framework.recipes.barriers.DistributedBarrier;

import java.util.concurrent.*;

public class Barrier extends BaseConfiguration {
    private final String path = "/barrier/test";
    private static int threadCount = 10;

    {
        client.start();
    }

    private void barrier() throws Exception {
        final DistributedBarrier barrier = new DistributedBarrier(client, path);
        barrier.setBarrier();

        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        Runnable runnable = new Runnable() {
            public void run() {
                DistributedBarrier barrier = new DistributedBarrier(client, path);
                try {
                    Thread.sleep((long)(5000 * Math.random()));
                    System.out.println(Thread.currentThread().getName() + " enter barrier");
                    barrier.waitOnBarrier();
                    System.out.println(Thread.currentThread().getName() + " leave barrier");
                    Thread.sleep(1000);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };
        for (int i = 0; i < threadCount; ++i)
            service.submit(runnable);

        Thread.sleep(100);
        System.out.println(Thread.currentThread().getName() + " is waiting...");
        Thread.sleep(10000);
        System.out.println(Thread.currentThread().getName() + " remove barrier");
        barrier.removeBarrier();
        System.out.println(Thread.currentThread().getName() + " wait to stop");
        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);
    }

    public static void main(String[] args) throws Throwable {
        new Barrier().barrier();
    }
}
