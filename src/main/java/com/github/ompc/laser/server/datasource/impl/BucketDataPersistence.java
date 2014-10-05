package com.github.ompc.laser.server.datasource.impl;

import com.github.ompc.laser.server.datasource.DataPersistence;
import com.github.ompc.laser.server.datasource.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.github.ompc.laser.common.LaserUtils.unmap;

/**
 * 分桶数据持久化
 * Created by vlinux on 14-10-4.
 * @deprecated 使用更高效的PageDataPersistence
 */
public class BucketDataPersistence implements DataPersistence {

    private final static int BUCKET_ROWS_SIZE = 1024 * 512;//每个数据桶大小
    private final static int BUCKET_SIZE = 20;//桶总数
    private final static byte[] ENDS = new byte[]{'\r', '\n'};

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final File dataFile;
    private final Bucket[] buckets = new Bucket[BUCKET_SIZE];
    private final ReentrantLock writeLock = new ReentrantLock();
    private final Condition writeCondition = writeLock.newCondition();

    private boolean isFlushed = false;//是否被标记为finished,如果被标记则需要强刷最后一个未完成的桶
    private final CountDownLatch finishCountDown = new CountDownLatch(1);
    private final ArrayList<MappedByteBuffer> waitForFlushBuffers = new ArrayList<>();

    final Runnable writer = new Runnable() {

        @Override
        public void run() {

            // 上次完成的桶编号
            int lastFinishedBucketIndex = -1;

            // 累计写入的文件大小
            long pos = 0;

            // 检查文件是否存在，不存在则创建
            if (!dataFile.exists()) {
                try {
                    dataFile.createNewFile();
                } catch (IOException e) {
                    log.warn("create dataFile={} failed.", dataFile, e);
                }
            }

            try (final FileChannel fileChannel = new RandomAccessFile(dataFile, "rw").getChannel()) {

                while (!isFlushed) {

                    writeLock.lock();
                    try {
                        writeCondition.await();
                    } catch (InterruptedException e) {
                        // do nothing...
                    } finally {
                        writeLock.unlock();
                    }//try

                    // 从上次已完成的桶接着往下开始遍历
                    for (int i = lastFinishedBucketIndex + 1; i < BUCKET_SIZE; i++) {

                        final Bucket bucket = buckets[i];
                        if (bucket.isFinished) {
                            lastFinishedBucketIndex = i;
                            continue;
                        }

                        if (bucket.isFull()
                                || (isFlushed && !bucket.isEmpty())) {
                            final long byteCount = bucket.byteCount.get();
                            final int rowCount = bucket.rowCount.get();
                            final Row[] rows = bucket.rows;
                            final MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, pos, byteCount);

                            for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                                final Row row = rows[rowIdx];

                                if( null == buffer ) {
                                    log.info("debug for null, pos={}, byteCount={}",pos, byteCount);
                                }

                                buffer.put(String.valueOf(row.getLineNum()).getBytes());
                                buffer.put(row.getData());
                                buffer.put(ENDS);
                            }

                            waitForFlushBuffers.add(buffer);

//                            buffer.force();
//                            unmap(buffer);

                            // 计数
                            pos += byteCount;
                            lastFinishedBucketIndex = i;

                            // 刷完一个桶则释放一个
                            buckets[i] = null;
                            log.info("bucket[{}] was finished, size={}", i, byteCount);
                        } else {
                            // 只要顺序上有一个桶未被填满则立即中断循环，等待下次唤醒
                            break;
                        }

                    }

                }//while

                finishCountDown.countDown();

            } catch (IOException e) {
                log.warn("BucketDataPersistence-writer write failed.", e);
            }

        }
    };

    public BucketDataPersistence(File dataFile) {
        this.dataFile = dataFile;
    }


    @Override
    public void putRow(Row row) throws IOException {

        final int bIdx = row.getLineNum() / BUCKET_ROWS_SIZE;
        final int cIdx = row.getLineNum() % BUCKET_ROWS_SIZE;

        final Bucket bucket = buckets[bIdx];
        bucket.rows[cIdx] = row;
        bucket.rowCount.incrementAndGet();

        final byte[] lineNumBytes = String.valueOf(row.getLineNum()).getBytes();
        bucket.byteCount.addAndGet(
                lineNumBytes.length
                        + row.getData().length
                        + 2//\r\n
        );

        // 如果桶被装满了则需要唤醒写入线程
        if (bucket.isFull()) {
            writeLock.lock();
            try {
                writeCondition.signal();
            } finally {
                writeLock.unlock();
            }//try
        }

    }

    @Override
    public void init() throws IOException {

        // 初始化桶
        for (int index = 0; index < buckets.length; index++) {
            buckets[index] = new Bucket();
        }

        // 初始化写入线程
        new Thread(writer, "BucketDataPersistence-writer").start();

        log.info("BucketDataPersistence(file:{}) was inited", dataFile);

    }

    @Override
    public void flush() throws IOException {
        isFlushed = true;
        writeLock.lock();
        try {
            writeCondition.signal();
        } finally {
            writeLock.unlock();
        }//try

        try {
            finishCountDown.await();
        } catch (InterruptedException e) {
            // ingore...
        }

        for( MappedByteBuffer buffer : waitForFlushBuffers ) {
            buffer.force();
            unmap(buffer);
        }

        log.info("BucketDataPersistence(file:{}) was flushed.", dataFile);
    }

    @Override
    public void destroy() throws IOException {
        log.info("BucketDataPersistence(file:{}) was destroyed.", dataFile);
    }

    /**
     * 数据桶
     */
    private class Bucket {

        private Row[] rows = new Row[BUCKET_ROWS_SIZE];//桶中数据
        private AtomicLong byteCount = new AtomicLong(0);//桶字节大小
        private volatile boolean isFinished = false;//标记为已完成,被writer线程写入文件
        private AtomicInteger rowCount = new AtomicInteger(0);//桶中数据个数

        /**
         * 判断桶是否已经装满
         *
         * @return
         */
        public boolean isFull() {
            return rowCount.get() == BUCKET_ROWS_SIZE;
        }

        /**
         * 判断桶是否从未装过数据
         *
         * @return
         */
        public boolean isEmpty() {
            return rowCount.get() == 0;
        }

    }

}
