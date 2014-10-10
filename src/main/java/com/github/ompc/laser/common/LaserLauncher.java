package com.github.ompc.laser.common;

import com.github.ompc.laser.client.ClientConfiger;
import com.github.ompc.laser.client.LaserClient;
import com.github.ompc.laser.client.NioLaserClient;
import com.github.ompc.laser.server.LaserServer;
import com.github.ompc.laser.server.NioLaserServer;
import com.github.ompc.laser.server.ServerConfiger;
import com.github.ompc.laser.common.datasource.DataPersistence;
import com.github.ompc.laser.common.datasource.DataSource;
import com.github.ompc.laser.common.datasource.impl.BucketDataPersistence;
import com.github.ompc.laser.common.datasource.impl.MappingDataSource;
import com.github.ompc.laser.common.datasource.impl.PageDataPersistence;
import com.github.ompc.laser.common.datasource.impl.PageDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Runtime.getRuntime;
import static java.lang.Thread.currentThread;

/**
 * Laser启动器
 * Created by vlinux on 14-10-3.
 */
public class LaserLauncher {

    private static final Logger log = LoggerFactory.getLogger(LaserLauncher.class);

    /**
     * 启动服务器
     *
     * @param args
     */
    private static void startServer(String... args) throws IOException, InterruptedException {

        final ServerConfiger configer = new ServerConfiger();
        configer.setDataFile(new File(args[1]));
        configer.setPort(Integer.valueOf(args[2]));

        final LaserOptions options = new LaserOptions(new File(args[3]));

        final DataSource dataSource = new MappingDataSource(configer.getDataFile());
        dataSource.init();

        final CountDownLatch countDown = new CountDownLatch(1);
        final ExecutorService executorService = Executors.newCachedThreadPool();

        final LaserServer server = new LaserServer(dataSource, countDown, executorService, configer, options);
        server.startup();

        // registe shutdown
        getRuntime().addShutdownHook(new Thread(() -> {
            currentThread().setName("server-shutdown-hook");
            try {
                dataSource.destroy();
                server.shutdown();
                executorService.shutdown();
            } catch (IOException e) {
                // do nothing...
            }
        }));

        countDown.await();
    }

    /**
     * 启动客户端
     *
     * @param args
     */
    private static void startClient(String... args) throws IOException, InterruptedException {

        final long startTime = System.currentTimeMillis();
        final ClientConfiger configer = new ClientConfiger();
        configer.setServerAddress(new InetSocketAddress(args[1], Integer.valueOf(args[2])));
        configer.setDataFile(new File(args[3]));

        final LaserOptions options = new LaserOptions(new File(args[4]));
        final int worksNum = options.getClientWorkNumbers();

        final CountDownLatch countDown = new CountDownLatch(worksNum);
        final ExecutorService executorService = Executors.newCachedThreadPool((r) -> {
            final Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        final DataPersistence dataPersistence = new BucketDataPersistence(configer.getDataFile());
        dataPersistence.init();

        // 建立链接
        final Set<LaserClient> clients = new HashSet<>();
        for (int i = 0; i < worksNum; i++) {
            final LaserClient client = new LaserClient(countDown, executorService, dataPersistence, configer, options);
            client.connect();
            clients.add(client);
        }

        // 驱动干活
        for (LaserClient client : clients) {
            client.work();
        }


        // 等待所有Client完成
        countDown.await();

        final long endTime = System.currentTimeMillis();
        System.out.println("cost=" + (endTime - startTime));

        // 刷新结果
        dataPersistence.flush();
        dataPersistence.destroy();

        // registe shutdown
        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                currentThread().setName("client-shutdown-hook");
                for (LaserClient client : clients) {
                    client.disconnect();
                }
                executorService.shutdown();
            } catch (IOException e) {
                // do nothing...
            }
        }));

    }

    /**
     * 启动NIO客户端
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void startNioClient(String... args) throws IOException, InterruptedException {

        final long startTime = System.currentTimeMillis();
        final ClientConfiger configer = new ClientConfiger();
        configer.setServerAddress(new InetSocketAddress(args[1], Integer.valueOf(args[2])));
        configer.setDataFile(new File(args[3]));

        final LaserOptions options = new LaserOptions(new File(args[4]));
        final int worksNum = options.getClientWorkNumbers();

        final CountDownLatch countDown = new CountDownLatch(worksNum);

        final CyclicBarrier workCyclicBarrier = new CyclicBarrier(worksNum + 1);

        final ExecutorService executorService = Executors.newCachedThreadPool((r) -> {
            final Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        final DataPersistence dataPersistence
                // = new BucketDataPersistence(configer.getDataFile())
                = new PageDataPersistence(configer.getDataFile());

        // 异步初始化数据源
        executorService.execute(() -> {

            try {
                dataPersistence.init();
                try {
                    workCyclicBarrier.await();
                } catch (Exception e) {
                    // ingore...
                }
            } catch (IOException e) {
                log.warn("DataPersistence.init failed.");
            }

        });


        // 异步创建建立链接
        final Set<NioLaserClient> clients = new HashSet<>();
        for (int i = 0; i < worksNum; i++) {

            executorService.execute(() -> {
                final NioLaserClient client = new NioLaserClient(countDown, workCyclicBarrier, executorService, dataPersistence, configer, options);
                try {
                    client.connect();
                    clients.add(client);
                    client.work();
                } catch (IOException e) {
                    log.warn("client connect failed.", e);
                }

            });

        }

        // 等待所有Client完成
        countDown.await();

        final long endTime = System.currentTimeMillis();
        System.out.println("cost=" + (endTime - startTime));

        // 刷新结果
        dataPersistence.flush();
        dataPersistence.destroy();

        // registe shutdown
        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                currentThread().setName("client-shutdown-hook");
                for (NioLaserClient client : clients) {
                    client.disconnect();
                }
                executorService.shutdown();
            } catch (IOException e) {
                // do nothing...
            }
        }));

    }

    /**
     * 启动NIO服务器
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void startNioServer(String... args) throws IOException, InterruptedException {
        final ServerConfiger configer = new ServerConfiger();
        configer.setDataFile(new File(args[1]));
        configer.setPort(Integer.valueOf(args[2]));

        final LaserOptions options = new LaserOptions(new File(args[3]));

        final DataSource dataSource
                // = new MappingDataSource(configer.getDataFile())
                // = new BlockDataSource(configer.getDataFile())
                = new PageDataSource(configer.getDataFile());
        dataSource.init();

        final CountDownLatch countDown = new CountDownLatch(1);//read&write
        final ExecutorService executorService = Executors.newCachedThreadPool();

        final NioLaserServer server = new NioLaserServer(dataSource, executorService, configer, options);
        server.startup();

        // registe shutdown
        getRuntime().addShutdownHook(new Thread(() -> {
            currentThread().setName("server-shutdown-hook");
            try {
                dataSource.destroy();
                server.shutdown();
                executorService.shutdown();
            } catch (IOException e) {
                // do nothing...
            }
        }));

        countDown.await();
    }

    public static void main(String... args) throws IOException, InterruptedException {

        if (args[0].equals("server")) {
            startServer(args);
        } else if (args[0].equals("client")) {
            startClient(args);
        } else if (args[0].equals("nioclient")) {
            startNioClient(args);
        } else if (args[0].equals("nioserver")) {
            startNioServer(args);
        } else {
            throw new IllegalArgumentException("illegal args[0]=" + args[0]);
        }

    }

}
