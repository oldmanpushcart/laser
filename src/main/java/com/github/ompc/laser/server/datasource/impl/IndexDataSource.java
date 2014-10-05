package com.github.ompc.laser.server.datasource.impl;

import com.github.ompc.laser.server.datasource.DataSource;
import com.github.ompc.laser.server.datasource.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.System.arraycopy;

/**
 * 索引数据源
 * Created by vlinux on 14-10-4.
 * @deprecated 性能不行
 */
public class IndexDataSource implements DataSource {

    private final static int BUFFER_SIZE = 512 * 1024 * 1024;//256M,4K倍数

    private final Logger log = LoggerFactory.getLogger(getClass());


    private final File dataFile;
    private final ConcurrentLinkedQueue<RowIndex> rowIndexQueue = new ConcurrentLinkedQueue<>();
    private ArrayList<MappedByteBuffer> mappedByteBuffers = new ArrayList<>();


    public IndexDataSource(File dataFile) {
        this.dataFile = dataFile;
    }

    @Override
    public Row getRow() throws IOException {
        final RowIndex rowIndex = rowIndexQueue.poll();
        if (null == rowIndex) {
            // arrive EOF
            return new Row(-1, new byte[0]);
        }
        return getRowByIndex(rowIndex);
    }

    /**
     * 根据索引获取row
     * @param rowIndex
     * @return
     */
    private Row getRowByIndex(RowIndex rowIndex) {
        final byte[] data = new byte[rowIndex.dataLength];
        int pos = 0;
        for( BufferIndex bufferIndex : rowIndex.bufferIndexes ) {
            final MappedByteBuffer mappedByteBuffer = mappedByteBuffers.get(bufferIndex.index);
            final ByteBuffer byteBuffer;
            synchronized (mappedByteBuffer) {
                byteBuffer = mappedByteBuffer.duplicate();
            }
            byteBuffer.position(bufferIndex.offset);
            byteBuffer.get(data, pos, bufferIndex.length);
            pos+=bufferIndex.length;
        }
        return new Row(rowIndex.lineNum, data);
    }

    @Override
    public void init() throws IOException {

        final long startTime = System.currentTimeMillis();

        int lineCounter = 0;//行计数器

        try (final FileChannel fileChannel = new RandomAccessFile(dataFile, "r").getChannel()) {

            int currentBufferIdx = 0;//当前缓存IDX,在切换buffer的时候++
            int currentBufferPos = 0;//当前缓存POS,在切换buffer的时候清0
            int currentBufferOffset = 0;//当前缓存offset,在切换buffer的时候清0
            int currentRowDataLength = 0;//当前行数据大小,在每一行结束的时候清0
            int currentRowDataLengthInCurrentBuffer = 0;//当前行数据在当前buffer的大小,在每一行结束或切换buffer的时候清0
            final ArrayList<BufferIndex> currentBufferIndexs = new ArrayList<>();//当前缓存索引片段,在每一行结束的时候清理
            boolean isAcrossBuffer = false;//是否跨buffer,在每一行结束的时候置false,但在每次decode的时候置true

            int bufferIndexCounter = 0;//缓存索引计数器,在切换buffer的时候++
            long filePos = 0;//文件pos
            final long fileSize = fileChannel.size();
            while (filePos < fileSize) {

                if( isAcrossBuffer ) {
                    // 如果当前跨buffer,需要将上一个buffer的pos快照保存
                    currentBufferIndexs.add(new BufferIndex(currentBufferIdx, currentBufferOffset, currentRowDataLengthInCurrentBuffer));
                }

                final MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, filePos, fixBufferSize(filePos, fileSize)).load();

                // switch buffer, reset current*
                currentBufferPos = 0;
                currentRowDataLengthInCurrentBuffer = 0;
                currentBufferOffset = 0;
                mappedByteBuffers.add(currentBufferIdx=bufferIndexCounter++, buffer);




                // decode a line
                isAcrossBuffer = true;
                DecodeLineState state = DecodeLineState.READ_D;
                while (buffer.hasRemaining()) {
                    switch (state) {

                        // 处理数据
                        case READ_D: {
                            final byte b = buffer.get();
                            currentBufferPos++;
                            if (b == '\r') {
                                state = DecodeLineState.READ_R;
                            } else {
                                // readed data, ingore this
                                currentRowDataLength++;
                                currentRowDataLengthInCurrentBuffer++;
                                break;
                            }
                        }

                        // 处理\r
                        case READ_R: {
                            final byte b = buffer.get();
                            currentBufferPos++;
                            if (b != '\n') {
                                throw new IOException("illegal format, \\n did not behind \\r, b=" + b);
                            }
                            state = DecodeLineState.READ_N;
                        }

                        // 处理\n
                        case READ_N: {
                            state = DecodeLineState.READ_D;

                            // 到这里为一行的结束,开始蛋疼的处理文件索引
                            currentBufferIndexs.add(new BufferIndex(currentBufferIdx, currentBufferOffset, currentRowDataLengthInCurrentBuffer));
                            rowIndexQueue.offer(new RowIndex(lineCounter++, currentRowDataLength, currentBufferIndexs));

                            // reset
                            currentRowDataLengthInCurrentBuffer = 0;
                            currentRowDataLength = 0;
                            currentBufferOffset=currentBufferPos;
                            currentBufferIndexs.clear();


                            break;
                        }

                        default:
                            throw new IOException("init failed, illegal state=" + state);
                    }
                }//while

                filePos += buffer.capacity();

            }

        }
        final long endTime = System.currentTimeMillis();
        log.info("DataSource(file:{}) was inited, cost={}", dataFile, (endTime - startTime));

    }

    @Override
    public void destroy() throws IOException {
        rowIndexQueue.clear();
        mappedByteBuffers.clear();
        //TODO : 手工释放MappedByteBuffers
        log.info("DataSource(file:{}) was destroyed.", dataFile);
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

    /**
     * 行解析状态机
     */
    private enum DecodeLineState {
        READ_D, // 读取数据
        READ_R, // 读取\r
        READ_N, // 读取\n
    }

    /**
     * 行索引
     */
    private class RowIndex {

        private int lineNum;
        private int dataLength;
        private ArrayList<BufferIndex> bufferIndexes = new ArrayList<>();

        private RowIndex(int lineNum, int dataLength, ArrayList<BufferIndex> bufferIndexes) {
            this.lineNum = lineNum;
            this.dataLength = dataLength;
            this.bufferIndexes.addAll(bufferIndexes);
        }

        /**
         * 判断当前行是否跨Buffer
         *
         * @return
         */
        public boolean isInSameBuffer() {
            return bufferIndexes.size() == 1;
        }

    }

    /**
     * 缓存索引
     */
    private class BufferIndex {

        private int index;
        private int offset;
        private int length;

        private BufferIndex(int index, int offset, int length) {
            this.index = index;
            this.offset = offset;
            this.length = length;
        }

    }

}
