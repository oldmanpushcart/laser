package com.github.ompc.laser.common.datasource.impl;

import com.github.ompc.laser.common.LaserUtils;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.github.ompc.laser.common.LaserUtils.unmap;
import static java.lang.Thread.currentThread;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

/**
 * 分页数据源<br/>
 * 目前有BUG，在分页切换的时候会丢行...
 * Created by vlinux on 14-10-5.
 */
public class PageDataSource implements DataSource {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final File dataFile;

    /*
     * 空行,避免过多的对象分配
     */
    private final static Row EMPTY_ROW = new Row(-1, new byte[0]);

    /*
     * 缓存页大小,要求是4K倍数
     */
    private final static int BUFFER_SIZE = 512 * 1024 * 1024;

    /*
     * 页行大小<br/>
     * 一行数据构成：有效字节数(4B)+数据段(210B)+填充段(42B) = 256B
     */
    private final int PAGE_ROW_SIZE = 256;

    /*
     * 页行数<br/>
     * 一页中总共有几行
     */
    private final int PAGE_ROWS_NUM =
            3000000
            //100000
            ;

    /*
     * 页码表大小<br/>
     * 一共有几页
     */
    private final int PAGE_TABLE_SIZE = 4;

    /*
     * 页码表
     */
    private Page[] pageTable = new Page[PAGE_TABLE_SIZE];

    /*
     * 标记是否曾经有其他线程到达过EOF状态
     */
    private volatile boolean isEOF = false;

    /*
     * 页面切换者锁
     */
    private final ReentrantLock pageSwitchLock = new ReentrantLock();
    private final Condition pageSwitchWakeUpCondition = pageSwitchLock.newCondition();


    public PageDataSource(File dataFile) {
        this.dataFile = dataFile;
    }

    private volatile Page currentPage = null;

    @Override
    public Row getRow(Row row) throws IOException {
        if (isEOF) {
            row.setLineNum(EMPTY_ROW.getLineNum());
            row.setData(EMPTY_ROW.getData());
        }

        while (true) {

            final Page page = currentPage == null ? pageTable[0] : currentPage;
            final int readCount = page.readCount.get();
            final int rowCount = page.rowCount;

            if( readCount == rowCount ) {

                if( page.isLast ) {
                    row.setLineNum(EMPTY_ROW.getLineNum());
                    row.setData(EMPTY_ROW.getData());
                    return row;
                } else {
                    continue;
                }

            }

            if (!page.readCount.compareAndSet(readCount, readCount + 1)) {
                // 这里更新真心热...有啥好办法咧？
                // log.info("debug for page.readCount CAS. readCount={}",readCount);
                continue;
            }

            final int offsetOfRow = readCount * PAGE_ROW_SIZE;
            final ByteBuffer byteBuffer = ByteBuffer.wrap(page.data, offsetOfRow, PAGE_ROW_SIZE);
            final int lineNum = byteBuffer.getInt();
            final int validByteCount = byteBuffer.getInt();
            final byte[] data = new byte[validByteCount];
            byteBuffer.get(data);
            row.setLineNum(lineNum);
            row.setData(data);

            // 到了页末
            if (page.readCount.get() == rowCount) {

                if (page.isLast) {
                    isEOF = true;
                } else {

                    pageSwitchLock.lock();
                    try {
                        pageSwitchWakeUpCondition.signal();
                    } finally {
                        pageSwitchLock.unlock();
                    }

                    final int pageNum = page.pageNum;
                    final int nextPageIdx = (pageNum + 1) % PAGE_TABLE_SIZE;
                    while (pageTable[nextPageIdx].pageNum != pageNum + 1) {
                        // spin for switch
                    }
                    currentPage = pageTable[nextPageIdx];
                }

            }

            return row;

        }
    }

    @Override
    public void init() throws IOException {

        /*
         * 页面切换者<br/>
         * 切换页码表中已完成的页面
         */
        final Thread pageSwitcher = new Thread(() -> {

            // 初始化页码表
            for (int i = 0; i < pageTable.length; i++) {
                final Page page = new Page();
                page.pageNum = i;
                pageTable[i] = page;
            }

            // 下一次要替换掉的页码表编号(0~PAGE_TABLE_SIZE)
            int nextSwitchPageTableIndex = 0;

            // 文件读取偏移量
            long fileOffset = 0;

            try (final FileChannel fileChannel = new RandomAccessFile(dataFile, "r").getChannel()) {

                // 文件整体大小
                final long fileSize = fileChannel.size();

                // 行号计数器
                int lineCounter = 0;

                // 文件缓存
                MappedByteBuffer mappedBuffer = null;

                // 行解析状态机
                DecodeLineState state = DecodeLineState.READ_D;

                while (fileOffset < fileSize) {

                    // 遍历页码表，主要做两件事
                    // 1.顺序的更换页码
                    // 2.将文件缓存刷入页码
                    final Page page = pageTable[nextSwitchPageTableIndex];

                    if (page.isInit
                            && page.readCount.get() < page.rowCount) {
                        // 如果已经被初始化后的当前页还没被读完,休眠等待被唤醒
                        pageSwitchLock.lock();
                        try {
                            pageSwitchWakeUpCondition.await();
                            continue;
                        } catch (InterruptedException e) {
                            currentThread().interrupt();
                        } finally {
                            pageSwitchLock.unlock();
                        }//try
                    }

                    if (!page.isInit
                            || page.readCount.get() == page.rowCount) {

                        final ByteBuffer dataBuffer = ByteBuffer.wrap(page.data);

                        // 页面中的行号
                        int rowIdx = 0;

                        final ByteBuffer tempBuffer = ByteBuffer.allocate(PAGE_ROW_SIZE);

                        FILL_PAGE_LOOP:
                        while (true) {
                            // 只有页面尚未被填满的时候才需要开始填充

                            if (null == mappedBuffer
                                    || !mappedBuffer.hasRemaining()) {
                                // 如果文件缓存是第一次加载,或者已到达尽头,需要做一次切换映射
                                // 修正映射长度
                                final long fixLength = (fileOffset + BUFFER_SIZE >= fileSize) ? fileSize - fileOffset : BUFFER_SIZE;

                                if (null != mappedBuffer) {
                                    unmap(mappedBuffer);
                                }

                                if (fixLength > 0) {
                                    mappedBuffer = fileChannel.map(READ_ONLY, fileOffset, fixLength).load();
                                }
                            }

                            if (!mappedBuffer.hasRemaining()) {
                                // 如果刚切完的文件缓存就已经没数据了,说明到达了EOF
                                // 需要关闭页面切换者
                                // 将当前页标记为最后一页
                                page.isLast = true;
                                break;
                            }

                            while (mappedBuffer.hasRemaining()) {
                                switch (state) {
                                    case READ_D: {
                                        final byte b = mappedBuffer.get();
                                        fileOffset++;
                                        if (b == '\r') {
                                            state = DecodeLineState.READ_R;
                                        } else {
                                            tempBuffer.put(b);
                                            break;
                                        }
                                    }

                                    case READ_R: {
                                        final byte b = mappedBuffer.get();
                                        fileOffset++;
                                        if (b != '\n') {
                                            throw new IOException("illegal format, \\n did not behind \\r, b=" + b);
                                        }
                                        state = DecodeLineState.READ_N;
                                    }

                                    case READ_N: {
                                        state = DecodeLineState.READ_D;

                                        // 将临时缓存中的数据填入页中
                                        tempBuffer.flip();
                                        final int dataLength = tempBuffer.limit();
                                        dataBuffer.putInt(lineCounter++);

                                        final byte[] _data = new byte[dataLength];
                                        tempBuffer.get(_data);
                                        final byte[] __data = LaserUtils.process(_data);
                                        dataBuffer.putInt(__data.length);
                                        dataBuffer.put(__data);

                                        tempBuffer.clear();


                                        // 重新计算当前行偏移量
                                        if (++rowIdx == PAGE_ROWS_NUM) {
                                            // 一页已经被填满,跳出本次页面填充动作
                                            break FILL_PAGE_LOOP;
                                        }

                                        int offsetOfRow = rowIdx * PAGE_ROW_SIZE;
                                        dataBuffer.position(offsetOfRow);

                                        break;
                                    }

                                    default:
                                        throw new IOException("init failed, illegal state=" + state);
                                }//switch:state

                            }//while:MAPPED

                        }//while:FILL_PAGE_LOOP

                        // 重新计算页面参数
                        page.rowCount = rowIdx;
                        page.readCount.set(0);
                        log.info("page.pageNum={} was switched. fileOffset={},fileSize={},page.rowCount={};",
                                page.pageNum, fileOffset, fileSize, page.rowCount);

                        if (fileOffset == fileSize) {
                            page.isLast = true;
                            log.info("page.pageNum={} is last, page.readCount={}", page.pageNum, page.readCount.get());
                        }

                        if (page.isInit) {
                            // 对初始化的页面不需要累加页面编号
                            page.pageNum += PAGE_TABLE_SIZE;
                        } else {
                            page.isInit = true;
                        }

                        // 最后一步，别忘记更新下一次要替换的页码表号
                        nextSwitchPageTableIndex = (nextSwitchPageTableIndex + 1) % PAGE_TABLE_SIZE;

                    }

                }//while

            } catch (IOException ioe) {
                log.warn("mapping file={} failed.", dataFile, ioe);
            }

            log.info("PageDataSource(file:{}) was arrive EOF.", dataFile);

        }, "PageDataSource-PAGESWITCHER-daemon");
        pageSwitcher.setDaemon(true);
        pageSwitcher.start();

        log.info("PageDataSource(file:{}) was inited.", dataFile);

    }

    @Override
    public void destroy() throws IOException {
        log.info("PageDataSource(file:{}) was destroyed.", dataFile);
    }


    /**
     * 缓存页
     */
    class Page {

        /*
         * 页码
         */
        volatile int pageNum;

        /*
         * 页面总行数
         */
        volatile int rowCount = 0;

        /*
         * 已被读取行数
         */
        AtomicInteger readCount = new AtomicInteger(0);

        /*
         * 是否最后一页
         */
        volatile boolean isLast = false;

        /*
         * 当前页面是否已经被初始化<br/>
         * 因为第一次读取数据的时候需要页面切换者进行读取,可以说是预加载
         */
        volatile boolean isInit = false;

        /*
         * 数据段
         */
        byte[] data = new byte[PAGE_ROW_SIZE * PAGE_ROWS_NUM];

    }

    /**
     * 行解析状态机
     */
    private enum DecodeLineState {
        READ_D, // 读取数据
        READ_R, // 读取\r
        READ_N, // 读取\n
    }

}
