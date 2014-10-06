package com.github.ompc.laser.server.datasource.impl;

import com.github.ompc.laser.server.datasource.DataPersistence;
import com.github.ompc.laser.server.datasource.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.github.ompc.laser.common.LaserUtils.unmap;
import static java.lang.Thread.currentThread;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * 分页数据持久化实现
 * Created by vlinux on 14-10-5.
 */
public class PageDataPersistence implements DataPersistence {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final File dataFile;

    /**
     * 行分隔符
     */
    private final static byte[] LINE_DELIMITER = new byte[]{'\r', '\n'};

    /*
     * 页行大小<br/>
     * 一行数据构成：有效字节数(4B)+数据段(210B)+填充段(42B) = 256B
     */
    private final int PAGE_ROW_SIZE = 256;

    /*
     * 页行数<br/>
     * 一页中总共有几行
     */
    private final int PAGE_ROWS_NUM = 100000;

    /*
     * 页码表大小<br/>
     * 一共有几页
     */
    private final int PAGE_TABLE_SIZE = 10;


    /*
     * 页码表
     */
    private Page[] pageTable = new Page[PAGE_TABLE_SIZE];

    /*
     * 等待刷新缓存集合
     */
    private Map<Integer, MappedByteBuffer> waitingFlushBufferMap = new HashMap<>();

    private FileChannel fileChannel;

    /*
     * 页面切换者锁
     */
    private final ReentrantLock pageSwitchLock = new ReentrantLock();
    private final Condition pageSwitchWakeUpCondition = pageSwitchLock.newCondition();

    /*
     * 页面切换者完成标记
     */
    private final CountDownLatch pageSwitchDone = new CountDownLatch(1);

    /*
     * 持久化数据源是否被刷新
     */
    private volatile boolean isFlushFlag = false;


    public PageDataPersistence(File dataFile) {
        this.dataFile = dataFile;
    }

    @Override
    public void putRow(Row row) throws IOException {


        final int lineNum = row.getLineNum();

        // 计算页码
        final int pageNum = lineNum / PAGE_ROWS_NUM;

        // 计算页码表位置
        final int tableIdx = pageNum % PAGE_TABLE_SIZE;

        while (pageTable[tableIdx].infoRef.get().pageNum != pageNum) {
            // TODO : 优化自旋锁
            // 如果页码表中当前位置所存放的页面编码对应不上
            // 则认为页切换不及时，这里采用自旋等待策略，其实相当危险
            Thread.yield();
        }

        final Page page = pageTable[tableIdx];

        // 计算页面内行号
        final int rowNum = lineNum % PAGE_ROWS_NUM;

        final byte[] bytesOfLineNum = String.valueOf(lineNum).getBytes();

        // 计算row中有效大小(B)
        final int validByteCount = 0
                + bytesOfLineNum.length
                + row.getData().length
                + LINE_DELIMITER.length;

        // 计算当前row所在page.data中的offset
        final int offset = rowNum * PAGE_ROW_SIZE;

        // 刷入页中
        ByteBuffer.wrap(page.data, offset, PAGE_ROW_SIZE)
                .putInt(validByteCount)
                .put(bytesOfLineNum)
                .put(row.getData())
                .put(LINE_DELIMITER);


        // CAS更新页面信息
        while (true) {

            final PageInfo expectInfo = page.infoRef.get();

            if (expectInfo.rowCount >= PAGE_ROWS_NUM) {
                // can't happend
                log.info("debug for CAS, row.lineNum={},page.rowCount={},PAGE_ROWS_NUM={}",
                        new Object[]{row.getLineNum(), expectInfo.rowCount, PAGE_ROWS_NUM});
                break;
            }

            final PageInfo updateInfo = new PageInfo(
                    expectInfo.pageNum,
                    expectInfo.rowCount + 1,
                    expectInfo.byteCount + validByteCount
            );

            if (!page.infoRef.compareAndSet(expectInfo, updateInfo)) {
                continue;
            }

            if (PAGE_ROWS_NUM == expectInfo.rowCount) {
                // 如果一页被写满,则需要通知切换者
                pageSwitchLock.lock();
                try {
                    pageSwitchWakeUpCondition.signal();
                } finally {
                    pageSwitchLock.unlock();
                }
            }

            break;

        }

    }

    @Override
    public void init() throws IOException {

        // 检查文件是否存在，不存在则创建
        if (!dataFile.exists()) {
            try {
                dataFile.createNewFile();
            } catch (IOException e) {
                log.warn("create dataFile={} failed.", dataFile, e);
            }
        }

        // 打开文件句柄
        fileChannel = new RandomAccessFile(dataFile, "rw").getChannel();

        // 初始化页码表
        for (int i = 0; i < pageTable.length; i++) {
            final Page page = new Page();
            page.infoRef.set(new PageInfo(i, 0, 0));
            pageTable[i] = page;
        }

        /*
         * 页面切换者<br/>
         * 切换页码表中已完成的页面
         */
        final Thread pageSwitcher = new Thread(() -> {

            // 下一次要替换掉的页码表编号(0~PAGE_TABLE_SIZE)
            int nextSwitchPageTableIndex = 0;

            // 文件写入偏移量
            long fileOffset = 0;

            while (true) {

                // 遍历页码表，主要做两件事
                // 1.顺序的更换页码
                // 2.将页码刷入文件缓存
                final Page page = pageTable[nextSwitchPageTableIndex];

                if (page.infoRef.get().rowCount < PAGE_ROWS_NUM
                        && !isFlushFlag) {
                    // 如果当前页面没满,且当前不是刷新状态,则进入休眠期
                    pageSwitchLock.lock();
                    try {
                        // 休眠100ms,或被唤醒
                        pageSwitchWakeUpCondition.await(100, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        currentThread().interrupt();
                    } finally {
                        pageSwitchLock.unlock();
                    }//try
                }

                if (page.infoRef.get().rowCount == PAGE_ROWS_NUM
                        || (isFlushFlag && page.infoRef.get().rowCount > 0)) {

                    // 1.当前页面已经被写满
                    // 2.当前页面已被写,需要立即刷入文件缓存中
                    try {

                        // 写完文件缓存后丢入待刷新队列中
                        final MappedByteBuffer mappedBuffer = fileChannel.map(READ_WRITE, fileOffset, page.infoRef.get().byteCount);
                        final ByteBuffer dataBuffer = ByteBuffer.wrap(page.data);
                        final int rowCount = page.infoRef.get().rowCount;
                        for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                            // 当前行偏移量
                            final int offsetOfRow = rowIdx * PAGE_ROW_SIZE;
                            dataBuffer.position(offsetOfRow);
                            final int validByteCount = dataBuffer.getInt();

                            // 当前行数据偏移量
                            final int offsetOfRowData = offsetOfRow + Integer.BYTES;
                            mappedBuffer.put(page.data, offsetOfRowData, validByteCount);
                        }//for
                        waitingFlushBufferMap.put(page.infoRef.get().pageNum, mappedBuffer);

                        // CAS update page's info
                        // // 重设当前页码数据
                        while (true) {

                            final PageInfo expectInfo = page.infoRef.get();
                            final PageInfo updateInfo = new PageInfo(expectInfo.pageNum + PAGE_TABLE_SIZE, 0, 0);
                            if( ! page.infoRef.compareAndSet(expectInfo, updateInfo) ) {
                                continue;
                            }

                            break;

                        }

                        // 计算整体偏移量
                        fileOffset += mappedBuffer.capacity();

                    } catch (IOException e) {
                        // 如果写文件映射发生异常，则表明当前I/O出错需要下次尝试
                        log.warn("mapping file failed.", e);
                        continue;
                    }//try

                    // 最后一步，别忘记更新下一次要替换的页码表号
                    nextSwitchPageTableIndex = (nextSwitchPageTableIndex + 1) % PAGE_TABLE_SIZE;

                    continue;

                }


                if (isFlushFlag
                        && page.infoRef.get().rowCount == 0) {
                    break;
                }

            }//while

            pageSwitchDone.countDown();

        }, "PageDataPersistence-PAGESWITCHER-daemon");
        pageSwitcher.setDaemon(true);
        pageSwitcher.start();

        log.info("PageDataPersistence(file:{}) was inited", dataFile);

    }

    @Override
    public void flush() throws IOException {

        // 标记数据源为刷新状态
        isFlushFlag = true;

        pageSwitchLock.lock();
        try {
            // 唤醒页面切换者
            pageSwitchWakeUpCondition.signal();
        } finally {
            pageSwitchLock.unlock();
        }

        try {
            // 等待页面切换者将所有页码表中数据都刷入文件缓存中
            pageSwitchDone.await();
        } catch (InterruptedException e) {
            // ingore...
        }

        // 将文件缓存到磁盘
        waitingFlushBufferMap.forEach((k, v) -> {
            v.force();
            log.info("k={} was forced. size={}", k, v.capacity());
        });
        log.info("PageDataPersistence(file:{}) was flushed.", dataFile);

    }

    @Override
    public void destroy() throws IOException {
        // umap off-heap
        waitingFlushBufferMap.forEach((k, v) -> unmap(v));
        if (null != fileChannel) {
            fileChannel.close();
        }
        log.info("PageDataPersistence(file:{}) was destroyed.", dataFile);
    }


    /**
     * 缓存页信息
     */
    final class PageInfo {

        /*
         * 页码
         */
        final int pageNum;

        /*
         * 页面总行数
         */
        final int rowCount;

        /*
         * 页面总字节数
         */
        final long byteCount;

        PageInfo(int pageNum, int rowCount, long byteCount) {
            this.pageNum = pageNum;
            this.rowCount = rowCount;
            this.byteCount = byteCount;
        }
    }

    /**
     * 缓存页<br/>
     * <p>
     * 一页有10^6行
     */
    final class Page {

        /*
         * 缓存页信息(引用)
         */
        final AtomicReference<PageInfo> infoRef = new AtomicReference<>();

        /*
         * 数据段
         */
        final byte[] data = new byte[PAGE_ROW_SIZE * PAGE_ROWS_NUM];

    }

}
