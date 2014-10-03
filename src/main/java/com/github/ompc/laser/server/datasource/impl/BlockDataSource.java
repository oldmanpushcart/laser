package com.github.ompc.laser.server.datasource.impl;

import com.github.ompc.laser.server.datasource.DataSource;
import com.github.ompc.laser.server.datasource.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 同步阻塞数据源
 * Created by vlinux on 14-9-21.
 */
public class BlockDataSource implements DataSource {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final static int BUFFER_SIZE = 4 * 1024 * 1024 * 32;//4K倍数

    private final AtomicInteger lineCounter = new AtomicInteger(0);
    private final File dataFile;
    private BufferedReader reader;

    public BlockDataSource(File dataFile) {
        this.dataFile = dataFile;
    }

    public void init() throws IOException {
        reader = new BufferedReader(new FileReader(dataFile), BUFFER_SIZE);
        log.info("DataSource(file:{}) was inited.", dataFile);
    }

    @Override
    public void destroy() throws IOException {
        if (null != reader) {
            reader.close();
            log.info("DataSource(file:{}) was destroyed.", dataFile);
        }
    }

    @Override
    public Row getRow() throws IOException {
        final String line = reader.readLine();
        if (null == line) {
            return new Row(-1,new byte[0]);
        }
        return new Row(lineCounter.getAndIncrement(), line.getBytes());
    }

}
