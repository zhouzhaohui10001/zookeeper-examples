package cn.timd.zookeeper;

import com.sun.xml.internal.stream.util.ThreadLocalBufferAllocator;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DoubleBarrier extends BaseConfiguration {
    private static final String path = "/double_barrier/test";
    private static final int threadCount = 10;

    {
        client.start();
    }

    public void doubleBarrier() throws InterruptedException {
        DistributedDoubleBarrier distributedDoubleBarrier = new DistributedDoubleBarrier(
                client, path, threadCount);

        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            service.submit(new Runnable() {
                public void run() {
                    DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(
                            client, path, threadCount);
                    try {
                        Thread.sleep((long)(10000 * Math.random()));
                        System.out.println(Thread.currentThread().getName() + " enter double barrier");
                        barrier.enter();
                        System.out.println(Thread.currentThread().getName() + " is working");
                        Thread.sleep((long)(10000 * Math.random()));
                        System.out.println(Thread.currentThread().getName() + " begin to leave double barrier");
                        barrier.leave();
                        System.out.println(Thread.currentThread().getName() + " leave double barrier");
                        Thread.sleep(2000);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
        }

        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);
    }

    public static void main(String[] args) throws Throwable {
        new DoubleBarrier().doubleBarrier();
    }
}
