package com.github.ompc.laser.server;

import com.github.ompc.laser.common.LaserOptions;
import com.github.ompc.laser.common.LaserUtils;
import com.github.ompc.laser.common.networking.GetDataResp;
import com.github.ompc.laser.common.networking.GetEofResp;
import com.github.ompc.laser.server.datasource.DataSource;
import com.github.ompc.laser.server.datasource.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.ompc.laser.common.LaserConstant.PRO_REQ_GETDATA;
import static com.github.ompc.laser.common.LaserUtils.process;
import static com.github.ompc.laser.common.SocketUtils.format;
import static java.lang.Thread.currentThread;

/**
 * Nio实现的服务端
 * Created by vlinux on 14-10-4.
 */
public class NioLaserServer {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DataSource dataSource;
    private final CountDownLatch countDown;
    private final ExecutorService executorService;
    private final ServerConfiger configer;
    private final LaserOptions options;

    private ServerSocketChannel serverSocketChannel;
    private volatile boolean isRunning = true;
    private boolean isReaderRunning = true;
    private boolean isWriterRunning = true;

    public NioLaserServer(DataSource dataSource, CountDownLatch countDown, ExecutorService executorService, ServerConfiger configer, LaserOptions options) {
        this.dataSource = dataSource;
        this.countDown = countDown;
        this.executorService = executorService;
        this.configer = configer;
        this.options = options;
    }


    final Runnable accepter = new Runnable() {

        @Override
        public void run() {

            currentThread().setName("server-accepter");
            try (final Selector selector = Selector.open()) {

                serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
                while (isRunning) {

                    selector.select();
                    final Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                    while (iter.hasNext()) {
                        final SelectionKey key = iter.next();
                        iter.remove();

                        if (key.isAcceptable()) {
                            final SocketChannel socketChannel = serverSocketChannel.accept();
                            configSocketChannel(socketChannel);
                            new ChildHandler(socketChannel);
                            log.info("{} was connected.", format(socketChannel.socket()));
                        }

                    }//while

                }

            } catch (IOException ioe) {
                log.warn("server[port={}] accept failed.", configer.getPort(), ioe);
            }

        }

    };


    /**
     * Child处理器
     */
    private class ChildHandler {

        private final SocketChannel socketChannel;
        private final AtomicInteger reqCounter = new AtomicInteger(0);

        private ChildHandler(SocketChannel socketChannel) {
            this.socketChannel = socketChannel;
            executorService.execute(childReader);
            executorService.execute(childWriter);
        }

        final Runnable childReader = new Runnable() {

            @Override
            public void run() {

                currentThread().setName("child-" + format(socketChannel.socket()) + "-reader");

                final ByteBuffer buffer = ByteBuffer.allocateDirect(options.getServerChildReceiverBufferSize());
                try (final Selector selector = Selector.open()) {

                    socketChannel.register(selector, SelectionKey.OP_READ);
                    while (isRunning
                            && isReaderRunning) {

                        selector.select();
                        final Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                        while (iter.hasNext()) {
                            final SelectionKey key = iter.next();
                            iter.remove();

                            if (key.isReadable()) {

                                socketChannel.read(buffer);
                                buffer.flip();

                                while (true) {
                                    if (buffer.remaining() < Integer.BYTES) {
                                        break;
                                    }

                                    final int type = buffer.getInt();
                                    if (type != PRO_REQ_GETDATA) {
                                        throw new IOException("decode failed, illegal type=" + type);
                                    }

                                    reqCounter.incrementAndGet();

                                }//while
                                buffer.compact();


                            }//if:readable

                        }//while:iter

                    }//while:MAIN_LOOP

                } catch (IOException ioe) {
                    log.info("{} was disconnect for read.", format(socketChannel.socket()));
                } finally {
                    isReaderRunning = false;
                }

            }

        };

        final Runnable childWriter = new Runnable() {

            @Override
            public void run() {

                currentThread().setName("child-" + format(socketChannel.socket()) + "-writer");

                final ByteBuffer buffer = ByteBuffer.allocateDirect(options.getServerChildSendBufferSize());
                try (final Selector selector = Selector.open()) {

                    final int LIMIT_REMAINING = 212;//TYPE(4B)+LINENUM(4B)+LEN(4B)+DATA(200B)
                    socketChannel.register(selector, SelectionKey.OP_WRITE);
                    while (isRunning
                            && isWriterRunning) {

                        boolean isEof = false;
                        while (buffer.remaining() >= LIMIT_REMAINING
                                && !isEof) {

                            while (reqCounter.get() > 0) {

                                if (buffer.remaining() < LIMIT_REMAINING) {
                                    // TODO : 目前这里利用了DATA长度不超过200的限制，没有足够的通用性，后续改掉
                                    break;
                                }

                                reqCounter.decrementAndGet();
                                final Row row = dataSource.getRow();

                                if (row.getLineNum() < 0) {
                                    // EOF
                                    final GetEofResp resp = new GetEofResp();
                                    buffer.putInt(resp.getType());
                                    isEof = true;
                                    break;
                                } else {
                                    // normal
                                    final GetDataResp resp = new GetDataResp(row.getLineNum(), process(row.getData()));
                                    buffer.putInt(resp.getType());
                                    buffer.putInt(resp.getLineNum());
                                    buffer.putInt(resp.getData().length);
                                    buffer.put(resp.getData());
                                }

                            }//while

                        }


                        // 这里似乎有点多余~
//                        socketChannel.register(selector, OP_WRITE);
                        buffer.flip();

                        selector.select();
                        final Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                        while (iter.hasNext()) {
                            final SelectionKey key = iter.next();
                            iter.remove();

                            if (key.isWritable()) {
//                                log.info("debug for write, pos={};remaining={}",
//                                        new Object[]{buffer.position(),buffer.remaining()});
                                final int count = socketChannel.write(buffer);
//                                log.info("debug for write, count={}",new Object[]{count});
                                buffer.compact();
//                                key.interestOps(key.interestOps() & ~OP_WRITE);
                            }

                        }//while:iter

                    }//while:MAIN_LOOP

                } catch (IOException ioe) {
                    log.info("{} was disconnect for write.", format(socketChannel.socket()));
                } finally {
                    isWriterRunning = false;
                }

            }

        };

    }


    /**
     * 获取并配置ServerSocketChannel
     *
     * @return
     * @throws IOException
     */
    private ServerSocketChannel getServerSocketChannel() throws IOException {
        final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.socket().setSoTimeout(options.getServerSocketTimeout());
        return serverSocketChannel;
    }

    /**
     * 配置SocketChannel
     *
     * @throws IOException
     */
    private void configSocketChannel(SocketChannel socketChannel) throws IOException {
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
    }

    /**
     * 启动服务端
     *
     * @throws IOException
     */
    public void startup() throws IOException {

        serverSocketChannel = getServerSocketChannel();
        serverSocketChannel.bind(new InetSocketAddress(configer.getPort()), options.getServerBacklog());

        executorService.execute(accepter);
        log.info("server[port={}] startup successed.", configer.getPort());
    }

    /**
     * 关闭服务端
     *
     * @throws IOException
     */
    public void shutdown() throws IOException {

        isRunning = false;
        if (null != serverSocketChannel) {
            serverSocketChannel.close();
        }

        log.info("server[port={}] shutdown successed.", configer.getPort());

    }

}
