package com.github.ompc.laser.client;

import com.github.ompc.laser.common.LaserOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * NIO版本的LaserClient
 * Created by vlinux on 14-10-3.
 */
public class NioLaserClient {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final CountDownLatch countDown;
    private final ExecutorService executorService;
    private final ClientConfiger configer;
    private final LaserOptions options;

    private SocketChannel socketChannel;


    public NioLaserClient(CountDownLatch countDown, ExecutorService executorService, ClientConfiger configer, LaserOptions options) {
        this.countDown = countDown;
        this.executorService = executorService;
        this.configer = configer;
        this.options = options;
    }

    /**
     * 获取并配置SocketChannel
     *
     * @return
     * @throws IOException
     */
    private SocketChannel getAndConfigSocketChannel() throws IOException {
        final SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        // config the socket
        final Socket socket = socketChannel.socket();
        socket.setTcpNoDelay(options.isClientTcpNoDelay());
        socket.setReceiveBufferSize(options.getClientReceiverBufferSize());
        socket.setSendBufferSize(options.getClientSendBufferSize());
        socket.setSoTimeout(options.getClientSocketTimeout());
        socket.setPerformancePreferences(
                options.getClientPerformancePreferences()[0],
                options.getClientPerformancePreferences()[1],
                options.getClientPerformancePreferences()[2]);
        socket.setTrafficClass(options.getClientTrafficClass());
        return socketChannel;
    }

    /**
     * 链接到网络
     *
     * @throws java.io.IOException
     */
    public void connect() throws IOException {
        socketChannel = getAndConfigSocketChannel();
        final Selector selector = Selector.open();
        socketChannel.register(selector, SelectionKey.OP_CONNECT);
        socketChannel.connect(configer.getServerAddress());


        // waiting for connect
        WAITING_FOR_CONNECT:
        for (; ; ) {
            selector.select();
            final Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
            while (iter.hasNext()) {
                final SelectionKey key = iter.next();
                iter.remove();

                if (key.isConnectable()) {
                    SocketChannel channel = (SocketChannel) key.channel();
                    if (channel.isConnectionPending()) {
                        // block until connect finished
                        channel.finishConnect();

                        break WAITING_FOR_CONNECT;
                    }

                }//if

            }//while
        }//for

    }


    /**
     * 断开网络链接
     *
     * @throws IOException
     */
    public void disconnect() throws IOException {

    }

}
