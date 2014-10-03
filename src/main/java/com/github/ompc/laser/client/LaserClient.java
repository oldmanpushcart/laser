package com.github.ompc.laser.client;

import com.github.ompc.laser.common.SocketUtils;
import com.github.ompc.laser.common.networking.GetDataReq;
import com.github.ompc.laser.common.networking.GetDataResp;
import com.github.ompc.laser.common.networking.GetEofResp;
import com.github.ompc.laser.common.networking.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static com.github.ompc.laser.common.SocketUtils.format;
import static com.github.ompc.laser.common.SocketUtils.write;

/**
 * 客户端
 * Created by vlinux on 14-9-30.
 */
public class LaserClient {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final CountDownLatch countDown;
    private final ExecutorService executorService;
    private final ClientConfiger configer;


    private Socket socket;
    private volatile boolean isRunning = true;

    public LaserClient(CountDownLatch countDown, ExecutorService executorService, ClientConfiger configer) throws IOException {
        this.countDown = countDown;
        this.executorService = executorService;
        this.configer = configer;
    }

    /**
     * 链接到网络
     *
     * @throws IOException
     */
    public void connect() throws IOException {
        socket = new Socket();

        // config the socket
        socket.setTcpNoDelay(true);

        socket.connect(configer.getServerAddress());
        final DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        final DataInputStream dis = new DataInputStream(socket.getInputStream());


        // init writer
        executorService.execute(() -> {
            Thread.currentThread().setName("client-" + format(socket) + "-writer");
            try {
                while (isRunning) {
                    write(dos, new GetDataReq());
                    dos.flush();
                }
            } catch (IOException ioe) {
                log.warn("{} write data failed.", format(socket), ioe);
            }
        });

        // init reader
        executorService.execute(() -> {
            Thread.currentThread().setName("client-" + format(socket) + "-reader");
            try {
                while (isRunning) {
                    final Protocol p = SocketUtils.read(dis);
                    if (p instanceof GetEofResp) {
                        countDown.countDown();
                        log.info("{} receive EOF.", format(socket));
                        break;
                    } else if (p instanceof GetDataResp) {
                        // TODO : write to ringbuffer
                    } else {
                        // can't happen
                    }
                }
            } catch (IOException ioe) {
                log.warn("{} read data failed.", format(socket), ioe);
            }
        });

        log.info("{} connect successed.", format(socket));
    }

    /**
     * 断开网络链接
     *
     * @throws IOException
     */
    public void disconnect() throws IOException {
        isRunning = false;
        if (null != socket) {
            socket.close();
        }
        log.info("{} disconnect successed.", format(socket));
    }


}
