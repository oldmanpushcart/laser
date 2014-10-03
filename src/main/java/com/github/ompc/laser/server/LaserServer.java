package com.github.ompc.laser.server;

import com.github.ompc.laser.common.LaserOptions;
import com.github.ompc.laser.common.networking.GetDataReq;
import com.github.ompc.laser.common.networking.GetDataResp;
import com.github.ompc.laser.common.networking.GetEofResp;
import com.github.ompc.laser.server.datasource.DataSource;
import com.github.ompc.laser.server.datasource.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.ompc.laser.common.SocketUtils.*;
import static java.lang.Thread.MIN_PRIORITY;
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
    private final LaserOptions options;

    private ServerSocket serverSocket;
    private volatile boolean isRunning = true;

    public LaserServer(DataSource dataSource, CountDownLatch countDown, ExecutorService executorService, ServerConfiger configer, LaserOptions options) throws IOException {
        this.countDown = countDown;
        this.dataSource = dataSource;
        this.executorService = executorService;
        this.configer = configer;
        this.options = options;
    }

    /**
     * 启动服务端
     *
     * @throws IOException
     */
    public void startup() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.setSoTimeout(options.getServerSocketTimeout());
        serverSocket.bind(new InetSocketAddress(configer.getPort()),options.getServerBacklog());

        // init accept thread
        executorService.execute(() -> {
            currentThread().setName("server-accept");
            try {
                while (isRunning) {
                    initClientHandler(serverSocket.accept());
                }
            } catch (IOException ioe) {
                log.warn("server[port={}] accept failed.", configer.getPort(), ioe);
            }
        });

        log.info("server[port={}] startup successed.", configer.getPort());
    }

    /**
     * 获取数据输出流
     * @param os
     * @return
     */
    private DataOutputStream getDataOutputStream(OutputStream os) {
        if( options.getServerChildSendCorkSize() <= 0 ) {
            return new DataOutputStream(os);
        } else {
            return new DataOutputStream(new BufferedOutputStream(os, options.getServerChildSendCorkSize()));
        }
    }

    /**
     * 初始化 client's socket 处理器
     *
     * @param socket
     * @throws IOException
     */
    private void initClientHandler(Socket socket) throws IOException {

        // config socket
        socket.setTcpNoDelay(options.isServerChildTcpNoDelay());
        socket.setReceiveBufferSize(options.getServerChildReceiverBufferSize());
        socket.setSendBufferSize(options.getServerChildSendBufferSize());
        socket.setSoTimeout(options.getServerSocketTimeout());
        socket.setPerformancePreferences(
                options.getServerChildPerformancePreferences()[0],
                options.getServerChildPerformancePreferences()[1],
                options.getServerChildPerformancePreferences()[2]);
        socket.setTrafficClass(options.getServerChildTrafficClass());

        final ConcurrentLinkedQueue<Row> rowQueue = new ConcurrentLinkedQueue<>();
        final DataOutputStream dos = getDataOutputStream(socket.getOutputStream());
        final DataInputStream dis = new DataInputStream(socket.getInputStream());
        final AtomicInteger reqCounter = new AtomicInteger(0);

        log.info("{} was connected.", format(socket));
        // init client handler's reader
        executorService.execute(() -> {
            currentThread().setName("server-" + format(socket) + "-reader");
            try {
                while (isRunning) {
                    final GetDataReq req = (GetDataReq) read(dis);
                    //TODO : check read protocol's type
//                    rowQueue.offer(dataSource.getRow());
                    reqCounter.incrementAndGet();

                }
            } catch (IOException ioe) {
                if( !socket.isClosed() ) {
                    log.warn("{} read data failed.", format(socket), ioe);
                }
            }
        });

        // init client handler's writer
        executorService.execute(() -> {
            currentThread().setName("server-" + format(socket) + "-writer");
            currentThread().setPriority(MIN_PRIORITY);
            try {
                while (isRunning) {

//                    final Row row = rowQueue.poll();
//                    if( null == row ) {
//                        Thread.yield();
//                        continue;
//                    }

                    while( reqCounter.get() > 0 ) {
                        final Row row = dataSource.getRow();
                        if (row.getLineNum() >= 0) {
                            write(dos, new GetDataResp(row.getLineNum(), row.getData()));
                            if( options.isServerSendAutoFlush() ) {
                                dos.flush();
                            }
                        } else {
                            write(dos, new GetEofResp());
                            dos.flush();
                        }//if
                        reqCounter.decrementAndGet();
                    }


                }
            } catch (IOException ioe) {
                if( !socket.isClosed() ) {
                    log.warn("{} read data failed.", format(socket), ioe);
                }
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
