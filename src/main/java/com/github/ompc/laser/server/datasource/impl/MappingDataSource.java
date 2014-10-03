package com.github.ompc.laser.server.datasource.impl;

import com.github.ompc.laser.server.datasource.DataSource;
import com.github.ompc.laser.server.datasource.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

/**
 * 内存映射数据源
 * Created by vlinux on 14-10-4.
 */
public class MappingDataSource implements DataSource {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final static int BUFFER_SIZE = 256 * 1024 * 1024;//256M,4K倍数

    private final File dataFile;

    /**
     * 默认数据源
     *
     * @param dataFile
     */
    public MappingDataSource(File dataFile) {
        this.dataFile = dataFile;
    }

    private MappedByteBuffer mapBuffer;
    private MappedByteBuffer nextBuffer;

    private FileChannel fileChannel;
    private long fileLength;
    private volatile int lineNum = 0;
    private long cursor = 0;

    private Runnable bufferLoader = () -> {

        log.info("{} was started.", Thread.currentThread().getName());

        while (true) {

            if (isEof()) {
                break;
            }

            final long nextCursor = cursor + mapBuffer.capacity();
            long nextFixBufferSize = BUFFER_SIZE;
            if (nextCursor + BUFFER_SIZE >= fileLength) {
                nextFixBufferSize = fileLength - nextCursor;
            }

            if (nextFixBufferSize <= 0) {
                break;
            }

            try {
                nextBuffer = switchBuffer(nextCursor, nextFixBufferSize);
            } catch (IOException e) {
                log.warn("mapping buffer failed. nextCursor={};nextFixBufferSize={};",
                        new Object[]{nextCursor, nextFixBufferSize, e});
            }//try

            synchronized (this) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    currentThread().interrupt();
                }
            }//sync

        }

    };

    /**
     * 是否文件末尾
     *
     * @return
     */
    private boolean isEof() {
        return cursor >= fileLength;
    }

    /**
     * 切换映射
     *
     * @param pos
     * @param size
     * @return
     * @throws IOException
     */
    private MappedByteBuffer switchBuffer(long pos, long size) throws IOException {
        final long startTime = System.currentTimeMillis();
        final MappedByteBuffer newBuffer = fileChannel.map(READ_ONLY, pos, size);
        newBuffer.load();
        final long finishTime = System.currentTimeMillis();
        log.info("DataSource(file:{}) switch mapping, pos={};BUFFER_SIZE={};cost={}ms",
                new Object[]{dataFile, cursor, newBuffer.capacity(), (finishTime - startTime)});
        return newBuffer;
    }

    /**
     * 修正缓存大小
     *
     * @return
     */
    private long fixBufferSize() {
        if (cursor + BUFFER_SIZE >= fileLength) {
            return fileLength - cursor;
        } else {
            return BUFFER_SIZE;
        }
    }

    /**
     * 状态机
     */
    public enum State {
        READ_R,
        READ_N
    }

    @Override
    public synchronized Row getRow() throws IOException {
        if (isEof()) {
            return new Row(-1, new byte[0]);
        }

        int pos = 0;
        final byte[] buf = new byte[1024];
        final Row data = new Row();
        State checkpoint = State.READ_R;

        WHILE:
        do {

            if (!mapBuffer.hasRemaining()) {

                cursor += mapBuffer.capacity();
                if (isEof()) {
                    log.warn("DataSource(file:{}) arrive EOF, pos={};", dataFile, cursor);
                    return new Row(-1, new byte[0]);
                }

                mapBuffer = nextBuffer;
                synchronized (bufferLoader) {
                    bufferLoader.notify();
                }

            }//if

            if (isEof()) {
                log.warn("DataSource(file:{}) arrive EOF, pos={};", dataFile, cursor);
                return new Row(-1, new byte[0]);
            }

            final byte b = mapBuffer.get();

            switch (checkpoint) {

                case READ_R:
                    if (b == '\r') {
                        checkpoint = State.READ_N;
                    } else {
                        buf[pos++] = b;
                    }
                    break;

                case READ_N:
                    if (b != '\n') {
                        throw new IllegalStateException("read file failed. format was not end with \\r\\n each line.");
                    }
                    data.setData(new byte[pos]);
                    data.setLineNum(lineNum++);
                    System.arraycopy(buf, 0, data.getData(), 0, pos);
                    break WHILE;

            }//switch

        } while (true);

        if (checkpoint != State.READ_N) {
            throw new IllegalStateException("read file failed. format was not end with \\r\\n each line.");
        }

        return data;
    }

    @Override
    public void init() throws IOException {

        if (null == dataFile
                || !dataFile.exists()
                || !dataFile.canRead()) {
            throw new FileNotFoundException(format("file=%s access failed.", dataFile));
        }

        final long startTime = System.currentTimeMillis();
        fileChannel = new FileInputStream(dataFile).getChannel();
        fileLength = fileChannel.size();
        cursor = 0;
        mapBuffer = switchBuffer(cursor, fixBufferSize());

        final Thread bufferLoaderDaemon = new Thread(bufferLoader, "MappingDataSourceLoaderDaemon");
        bufferLoaderDaemon.setDaemon(true);
        bufferLoaderDaemon.start();

        final long finishTime = System.currentTimeMillis();
        log.info("DataSource(file:{}) was inited, pos={};BUFFER_SIZE={};cost={}ms.",
                new Object[]{dataFile, cursor, mapBuffer.capacity(), (finishTime - startTime)});

    }

    @Override
    public void destroy() throws IOException {
        if (null != fileChannel) {
            try {
                fileChannel.close();
            } catch (Throwable t) {
            }
            log.info("DataSource(file:{}) was destroyed.", dataFile);
        }
    }

}
