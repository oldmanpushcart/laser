package com.github.ompc.laser.client;

import com.github.ompc.laser.common.LaserOptions;
import com.github.ompc.laser.common.channel.CompressReadableByteChannel;
import com.github.ompc.laser.common.datasource.DataPersistence;
import com.github.ompc.laser.common.datasource.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;

import static com.github.ompc.laser.common.LaserConstant.*;
import static com.github.ompc.laser.common.LaserUtils.reverse;
import static com.github.ompc.laser.common.SocketUtils.format;
import static java.lang.Thread.currentThread;
import static java.nio.channels.SelectionKey.OP_CONNECT;

/**
 * NIO版本的LaserClient
 * Created by vlinux on 14-10-3.
 */
public class NioLaserClient {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final CountDownLatch countDown;
    private final CyclicBarrier workCyclicBarrier;
    private final ExecutorService executorService;
    private final DataPersistence dataPersistence;
    private final ClientConfiger configer;
    private final LaserOptions options;

    private SocketChannel socketChannel;
    private volatile boolean isRunning = true;


    public NioLaserClient(CountDownLatch countDown, CyclicBarrier workCyclicBarrier, ExecutorService executorService, DataPersistence dataPersistence, ClientConfiger configer, LaserOptions options) {
        this.countDown = countDown;
        this.workCyclicBarrier = workCyclicBarrier;
        this.executorService = executorService;
        this.dataPersistence = dataPersistence;
        this.configer = configer;
        this.options = options;
    }

    /**
     * 获取并配置SocketChannel
     *
     * @return 返回SocketChannel
     * @throws IOException
     */
    private SocketChannel getAndConfigSocketChannel() throws IOException {
        final SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        // config the socket
        final Socket socket = socketChannel.socket();
        socket.setTcpNoDelay(options.isClientTcpNoDelay());
        socket.setReceiveBufferSize(options.getClientSocketReceiverBufferSize());
        socket.setSendBufferSize(options.getClientSocketSendBufferSize());
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

        socketChannel.connect(configer.getServerAddress());
        // waiting for connect
        try (final Selector selector = Selector.open()) {
            socketChannel.register(selector, OP_CONNECT);
            WAITING_FOR_CONNECT:
            for (; ; ) {
                selector.select();
                final Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    final SelectionKey key = iter.next();
                    iter.remove();

                    if (key.isConnectable()) {
                        final SocketChannel channel = (SocketChannel) key.channel();
                        if (channel.isConnectionPending()) {
                            // block until connect finished
                            channel.finishConnect();
                            break WAITING_FOR_CONNECT;
                        }
                    }//if

                }//while
            }//for

        }//try

        log.info("{} connect successed.", format(socketChannel.socket()));

    }

    /**
     * 开始干活
     *
     * @throws IOException
     */
    public void work() throws IOException {

        executorService.execute(() -> {

            currentThread().setName("client-" + format(socketChannel.socket()));
            final ByteBuffer writerBuffer = ByteBuffer.allocateDirect(options.getClientSendBufferSize());
            final ByteBuffer readerBuffer = ByteBuffer.allocateDirect(options.getClientReceiverBufferSize());
            final ReadableByteChannel readableByteChannel = options.isEnableCompress()
                    ? new CompressReadableByteChannel(socketChannel, options.getCompressSize())
                    : socketChannel;

            try (final Selector selector = Selector.open()) {

                try {
                    workCyclicBarrier.await();
                } catch (Exception e) {
                    log.warn("workCB await failed.", e);
                }


                // decode
                int type;
                int lineNum = 0;
                int len = 0;
                final Row row = new Row();
                ReaderDecodeState readerState = ReaderDecodeState.READ_TYPE;
                WriterDecodeState writerState = WriterDecodeState.WRITE_TYPE;

                MAIN_LOOP:
                while (isRunning) {

                    selector.select();
                    final Iterator<SelectionKey> iter = selector.selectedKeys().iterator();

                    while (iter.hasNext()) {
                        final SelectionKey key = iter.next();
                        iter.remove();

                        if (key.isWritable()) {
                            switch (writerState) {

                                case WRITE_TYPE: {
                                    writerBuffer.putInt(PRO_REQ_GETDATA);
                                    if( !writerBuffer.hasRemaining() ) {
                                        writerBuffer.flip();
                                        writerState = WriterDecodeState.WRITE_DATA;
                                    }
                                }

                                case WRITE_DATA: {
                                    socketChannel.write(writerBuffer);
                                    if( !writerBuffer.hasRemaining() ) {
                                        writerBuffer.compact();
                                        writerState = WriterDecodeState.WRITE_TYPE;
                                    }
                                }

                            }
                        }//if:writable

                        if (key.isReadable()) {

                            readableByteChannel.read(readerBuffer);
                            readerBuffer.flip();

                            switch (readerState) {

                                case READ_TYPE: {
                                    if (readerBuffer.remaining() < Integer.BYTES) {
                                        break;
                                    }
                                    type = readerBuffer.getInt();
                                    if (type == PRO_RESP_GETDATA) {
                                        readerState = ReaderDecodeState.READ_GETDATA_LINENUM;
                                    } else if (type == PRO_RESP_GETEOF) {
                                        readerState = ReaderDecodeState.READ_GETEOF;
                                        break;
                                    } else {
                                        throw new IOException("decode failed, illegal type=" + type);
                                    }
                                }

                                case READ_GETDATA_LINENUM: {
                                    if (readerBuffer.remaining() < Integer.BYTES) {
                                        break;
                                    }
                                    lineNum = readerBuffer.getInt();
                                    readerState = ReaderDecodeState.READ_GETDATA_LEN;
                                }

                                case READ_GETDATA_LEN: {
                                    if (readerBuffer.remaining() < Integer.BYTES) {
                                        break;
                                    }
                                    len = readerBuffer.getInt();
                                    readerState = ReaderDecodeState.READ_GETDATA_DATA;
                                }

                                case READ_GETDATA_DATA: {
                                    if (readerBuffer.remaining() < len) {
                                        break;
                                    }
                                    final byte[] data = new byte[len];
                                    readerBuffer.get(data);
                                    reverse(data);

                                    readerState = ReaderDecodeState.READ_TYPE;

                                    // handler GetDataResp
                                    // 由于这里没有做任何异步化操作,包括dataPersistence中也没有
                                    // 所以这里优化将new去掉,避免过多的对象分配
                                    row.setLineNum(lineNum);
                                    row.setData(data);
                                    dataPersistence.putRow(row);
                                    break;
                                }

                                case READ_GETEOF: {
                                    // 收到EOF，结束整个client
                                    isRunning = false;
                                    countDown.countDown();
                                    log.info("{} receive EOF.", format(socketChannel.socket()));
                                    break MAIN_LOOP;
                                }


                                default:
                                    throw new IOException("decode failed, illegal readerState=" + readerState);
                            }//switch

                        }//if:readable

                    }//while:iter

                }//while:isRunning


            } catch (IOException ioe) {
                if (!socketChannel.socket().isClosed()) {
                    log.warn("{} client handle failed.", format(socketChannel.socket()), ioe);
                }
            }

        });

    }

    /**
     * 断开网络链接
     *
     * @throws IOException
     */
    public void disconnect() throws IOException {

        isRunning = false;
        if (null != socketChannel) {
            socketChannel.close();
            log.info("{} disconnect successed.", format(socketChannel.socket()));
        } else {
            log.info("{} disconnect successed.");
        }

    }

    /**
     * 写数据编码
     */
    enum WriterDecodeState {
        WRITE_TYPE,
        WRITE_DATA
    }

    /**
     * 接收数据解码
     */
    enum ReaderDecodeState {
        READ_TYPE,
        READ_GETDATA_LINENUM,
        READ_GETDATA_LEN,
        READ_GETDATA_DATA,
        READ_GETEOF
    }

}