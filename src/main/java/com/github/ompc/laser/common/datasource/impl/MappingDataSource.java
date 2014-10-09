package com.github.ompc.laser.common.datasource.impl;

import com.github.ompc.laser.common.datasource.DataSource;
import com.github.ompc.laser.common.datasource.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.github.ompc.laser.common.LaserUtils.unmap;
import static java.lang.Runtime.getRuntime;

/**
 * 内存映射数据源
 * Created by vlinux on 14-10-4.
 *
 * @deprecated 使用PageDataSource
 */
public class MappingDataSource implements DataSource {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final static int BUFFER_SIZE = 512 * 1024 * 1024;//256M,4K倍数

    private final File dataFile;
    private final ConcurrentLinkedQueue<Row> rowQueue = new ConcurrentLinkedQueue<>();

    /**
     * 默认数据源
     *
     * @param dataFile
     */
    public MappingDataSource(File dataFile) {
        this.dataFile = dataFile;
    }


    @Override
    public Row getRow(Row row) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Row getRow() throws IOException {
        final Row row = rowQueue.poll();
        if (null == row) {
            // arrive EOF
            return new Row(-1, new byte[0]);
        }
        return row;
    }

    /**
     * 行解析状态机
     */
    private enum DecodeLineState {
        READ_D, // 读取数据
        READ_R, // 读取\r
        READ_N, // 读取\n
    }

    /**
     * 修正mapping的大小
     *
     * @param pos
     * @param fileSize
     * @return
     */
    private long fixBufferSize(long pos, long fileSize) {
        if (pos + BUFFER_SIZE >= fileSize) {
            return fileSize - pos;
        } else {
            return BUFFER_SIZE;
        }
    }

    @Override
    public void init() throws IOException {

        final long startTime = System.currentTimeMillis();
        int lineCounter = 0;
        try (final FileChannel fileChannel = new RandomAccessFile(dataFile, "r").getChannel()) {

            long pos = 0;
            final ByteBuffer dataBuffer = ByteBuffer.allocate(1024);
            final long fileSize = fileChannel.size();
            while (pos < fileSize) {

                // mapping
                final long loadStartTime = System.currentTimeMillis();
                final MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, pos, fixBufferSize(pos, fileSize)).load();
                final long loadEndTime = System.currentTimeMillis();
                log.info("DataSource(file:{}) loading..., pos={},size={},cost={}",
                        new Object[]{dataFile, pos, buffer.capacity(), (loadEndTime - loadStartTime)});

                DecodeLineState state = DecodeLineState.READ_D;
                while (buffer.hasRemaining()) {
                    switch (state) {
                        case READ_D: {
                            final byte b = buffer.get();
                            if (b == '\r') {
                                state = DecodeLineState.READ_R;
                            } else {
                                dataBuffer.put(b);
                                break;
                            }
                        }

                        case READ_R: {
                            final byte b = buffer.get();
                            if (b != '\n') {
                                throw new IOException("illegal format, \\n did not behind \\r, b=" + b);
                            }
                            state = DecodeLineState.READ_N;
                        }

                        case READ_N: {
                            state = DecodeLineState.READ_D;
                            dataBuffer.flip();

                            // put into rowQueue
                            final byte[] data = new byte[dataBuffer.limit()];
                            dataBuffer.get(data);
                            final Row row = new Row(lineCounter++, data);
                            rowQueue.offer(row);

                            // reset dataBuffer
                            dataBuffer.clear();
                            break;
                        }

                        default:
                            throw new IOException("init failed, illegal state=" + state);
                    }
                }//while

                pos += buffer.capacity();
                unmap(buffer);
                getRuntime().gc();

            }//while


        }//try
        final long endTime = System.currentTimeMillis();
        log.info("DataSource(file:{}) was inited, cost={}", dataFile, (endTime - startTime));

    }

    @Override
    public void destroy() {
        rowQueue.clear();
        log.info("DataSource(file:{}) was destroyed.", dataFile);
    }
}