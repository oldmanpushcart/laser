package com.github.ompc.laser.server;

import com.github.ompc.laser.common.LaserOptions;
import com.github.ompc.laser.common.channel.CompressWritableByteChannel;
import com.github.ompc.laser.common.datasource.DataSource;
import com.github.ompc.laser.common.datasource.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.ompc.laser.common.LaserConstant.*;
import static com.github.ompc.laser.common.SocketUtils.format;
import static java.lang.Thread.currentThread;

/**
 * Nio实现的服务端
 * Created by vlinux on 14-10-4.
 */
public class NioLaserServer {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final DataSource dataSource;
    private final ExecutorService executorService;
    private final ServerConfiger configer;
    private final LaserOptions options;

    private ServerSocketChannel serverSocketChannel;
    private volatile boolean isRunning = true;
    private boolean isReaderRunning = true;
    private boolean isWriterRunning = true;

    public NioLaserServer(DataSource dataSource, ExecutorService executorService, ServerConfiger configer, LaserOptions options) {
        this.dataSource = dataSource;
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
                currentThread().setPriority(Thread.MAX_PRIORITY);

                final ByteBuffer buffer = ByteBuffer.allocateDirect(options.getServerChildSendBufferSize());
                final WritableByteChannel writableByteChannel = options.isEnableCompress()
                        ? new CompressWritableByteChannel(socketChannel, options.getCompressSize())
                        : socketChannel;

                boolean isEOF = false;
                final Row row = new Row();
                try (final Selector selector = Selector.open()) {

                    final int LIMIT_REMAINING = 212;//TYPE(4B)+LINENUM(4B)+LEN(4B)+DATA(200B)
                    socketChannel.register(selector, SelectionKey.OP_WRITE);

                    DecodeState state = DecodeState.FILL_BUFF;
                    boolean isNeedSend = false;
                    while (isRunning
                            && isWriterRunning) {

                        switch (state) {

                            case FILL_BUFF: {

                                // 一进来就先判断是否到达了EOF，如果已经到达了则不需要访问数据源
                                if (isEOF) {
                                    reqCounter.decrementAndGet();
                                    buffer.putInt(PRO_RESP_GETEOF);
                                    isNeedSend = true;
                                } else {

                                    if (reqCounter.get() > 0) {
                                        reqCounter.decrementAndGet();
                                        dataSource.getRow(row);

                                        if (row.getLineNum() < 0) {
                                            buffer.putInt(PRO_RESP_GETEOF);
                                            isEOF = true;
                                            isNeedSend = true;
                                        } else {
                                            buffer.putInt(PRO_RESP_GETDATA);
                                            buffer.putInt(row.getLineNum());

                                            buffer.putInt(row.getData().length);
                                            buffer.put(row.getData());

                                            if (buffer.remaining() < LIMIT_REMAINING) {
                                                // TODO : 目前这里利用了DATA长度不超过200的限制，没有足够的通用性，后续改掉
                                                isNeedSend = true;
                                            }
                                        }
                                    }

                                }

                                // 前边层层处理之后是否需要发送
                                if (isNeedSend) {
                                    buffer.flip();
                                    state = DecodeState.SEND_BUFF;
                                    isNeedSend = false;
                                }
                                break;
                            }


                            case SEND_BUFF: {

                                selector.select();
                                final Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                                while (iter.hasNext()) {
                                    final SelectionKey key = iter.next();
                                    iter.remove();

                                    if (key.isWritable()) {
                                        while (buffer.hasRemaining()) {
//                                            if( writableByteChannel instanceof CompressWritableByteChannel ) {
//                                                ((CompressWritableByteChannel)writableByteChannel).write(buffer,isEOF);
//                                            } else {
//                                                writableByteChannel.write(buffer);
//                                            }
                                            writableByteChannel.write(buffer);
                                        }
                                        buffer.compact();
                                        state = DecodeState.FILL_BUFF;

//                                        if (!buffer.hasRemaining()) {
//                                            // 缓存中的内容发送完之后才跳转到填充
//                                            state = DecodeState.FILL_BUFF;
//                                            buffer.compact();
//                                        }

                                    }

                                }//while:iter

                                break;
                            }

                        }//switch:state

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
     * @return 返回配置好的ServerSocketChannel
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
        socket.setTcpNoDelay(options.isServerChildTcpNoDelay());
        socket.setReceiveBufferSize(options.getServerChildSocketReceiverBufferSize());
        socket.setSendBufferSize(options.getServerChildSocketSendBufferSize());
        socket.setSoTimeout(options.getServerChildSocketTimeout());
        socket.setPerformancePreferences(
                options.getServerChildPerformancePreferences()[0],
                options.getServerChildPerformancePreferences()[1],
                options.getServerChildPerformancePreferences()[2]);
        socket.setTrafficClass(options.getServerChildTrafficClass());
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


    /**
     * 发送数据解码
     */
    enum DecodeState {
        FILL_BUFF,
        SEND_BUFF
    }

}