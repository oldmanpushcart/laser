package com.github.ompc.laser.common;

import com.github.ompc.laser.client.ClientConfiger;
import com.github.ompc.laser.client.LaserClient;
import com.github.ompc.laser.server.LaserServer;
import com.github.ompc.laser.server.ServerConfiger;
import com.github.ompc.laser.server.datasource.DataSource;
import com.github.ompc.laser.server.datasource.impl.BlockDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
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

        final DataSource dataSource = new BlockDataSource(configer.getDataFile());
        dataSource.init();

        final CountDownLatch countDown = new CountDownLatch(1);
        final ExecutorService executorService = Executors.newCachedThreadPool();

        final LaserServer server = new LaserServer(dataSource, countDown, executorService, configer);
        server.startup();

        // registe shutdown
        getRuntime().addShutdownHook(new Thread(() -> {
            currentThread().setName("server-shutdown-hook");
            try {
                dataSource.destroy();
                server.shutdown();
                executorService.shutdownNow();
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
        final int worksNum = Runtime.getRuntime().availableProcessors();
        log.info("client's worksNum={}",worksNum);

        final ClientConfiger configer = new ClientConfiger();
        configer.setServerAddress(new InetSocketAddress(args[1], Integer.valueOf(args[2])));
        configer.setDataFile(new File(args[3]));

        final CountDownLatch countDown = new CountDownLatch(worksNum);
        final ExecutorService executorService = Executors.newCachedThreadPool((r)->{
            final Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        final Set<LaserClient> clients = new HashSet<>();
        for (int i = 0; i < worksNum; i++) {
            final LaserClient client = new LaserClient(countDown, executorService, configer);
            client.connect();
            clients.add(client);
        }



        countDown.await();
        final long endTime = System.currentTimeMillis();
        System.out.println("cost="+(endTime - startTime));

        // registe shutdown
        getRuntime().addShutdownHook(new Thread(() -> {
            try {
                currentThread().setName("client-shutdown-hook");
                for(LaserClient client : clients) {
                    client.disconnect();
                }
                executorService.shutdownNow();
            } catch (IOException e) {
                // do nothing...
            }
        }));

    }

    public static void main(String... args) throws IOException, InterruptedException {

        if (args[0].equals("server")) {
            startServer(args);
        } else if (args[0].equals("client")) {
            startClient(args);
        } else {
            throw new IllegalArgumentException("illegal args[0]=" + args[0]);
        }

    }

}
