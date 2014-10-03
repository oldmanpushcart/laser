package com.github.ompc.laser.server;

import com.github.ompc.laser.common.networking.GetDataReq;
import com.github.ompc.laser.common.networking.GetDataResp;
import com.github.ompc.laser.common.networking.GetEofResp;
import com.github.ompc.laser.server.datasource.DataSource;
import com.github.ompc.laser.server.datasource.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static com.github.ompc.laser.common.SocketUtils.*;
import static java.lang.Thread.currentThread;

/**
 * 服务端
 * Created by vlinux on 14-9-29.
 */
public class LaserServer {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final static int QUEUE_SIZE = 1024 * 1024;//1M

    private final DataSource dataSource;
    private final CountDownLatch countDown;
    private final ExecutorService executorService;
    private final ServerConfiger configer;

    private ServerSocket serverSocket;

    private volatile boolean isRunning = true;


    public LaserServer(DataSource dataSource, CountDownLatch countDown, ExecutorService executorService, ServerConfiger configer) throws IOException {
        this.countDown = countDown;
        this.dataSource = dataSource;
        this.executorService = executorService;
        this.configer = configer;
    }

    /**
     * 启动服务端
     *
     * @throws IOException
     */
    public void startup() throws IOException {
        serverSocket = new ServerSocket(configer.getPort());

        // init accept thread
        executorService.execute(() -> {
            currentThread().setName("server-accept");
            try {
                while (isRunning) {
                    initClientHandler(serverSocket.accept());
                }
            } catch (IOException ioe) {
                log.warn("server[port={}] accept failed.", ioe);
            }
        });

        log.info("server[port={}] startup successed.", configer.getPort());
    }

    /**
     * 初始化 client's socket 处理器
     *
     * @param socket
     * @throws IOException
     */
    private void initClientHandler(Socket socket) throws IOException {

        final BlockingQueue<Row> rowQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
        final DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        final DataInputStream dis = new DataInputStream(socket.getInputStream());

        log.info("{} was connected.", format(socket));
        // init client handler's reader
        executorService.execute(() -> {
            currentThread().setName("server-" + format(socket) + "-reader");
            try {
                while (isRunning) {
                    final GetDataReq req = (GetDataReq) read(dis);
                    //TODO : check read protocol's type
                    final Row row = dataSource.getRow();
                    rowQueue.put(row);
                }
            } catch (IOException ioe) {
                log.warn("{} read data failed.", format(socket), ioe);
            } catch (InterruptedException ie) {
                log.warn("{} put row into queue failed.", format(socket), ie);
            }
        });

        // init client handler's writer
        executorService.execute(() -> {
            currentThread().setName("server-" + format(socket) + "-writer");
            try {
                while (isRunning) {
                    final Row row = rowQueue.take();
                    if (row.getLineNum() >= 0) {
                        write(dos, new GetDataResp(row.getLineNum(), row.getData()));
                    } else {
                        write(dos, new GetEofResp());
                    }
                }
            } catch (IOException ioe) {
                log.warn("{} read data failed.", format(socket), ioe);
            } catch (InterruptedException ie) {
                log.warn("{} put row into queue failed.", format(socket), ie);
            }
        });
    }

    /**
     * 关闭服务端
     *
     * @throws IOException
     */
    public void shutdown() throws IOException {
        isRunning = false;
        if (null != serverSocket) {
            serverSocket.close();
        }
        log.info("server[port={}] shutdown successed.", configer.getPort());
    }


}
