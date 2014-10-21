package com.github.ompc.laser.common.datasource.impl;

import com.github.ompc.laser.common.datasource.DataSource;
import com.github.ompc.laser.common.datasource.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 调试用数据源
 * Created by vlinux on 14/10/21.
 */
public class MockDataSource implements DataSource {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final byte[] MOCK_DATA = "abCdefGhijabCdefGhijabCdefGhijabCdefGhijabCdefGhijabCdefGhijabCdefGhijabCdefGhijabCdefGhijabCdefGhijabCdefGhijabCdefGhij".getBytes();

    @Override
    public Row getRow(Row row) throws IOException {
        row.setLineNum(1);
        row.setData(MOCK_DATA);
        return row;
    }

    @Override
    public void init() throws IOException {
        log.info("MockDataSource() was inited.");
    }

    @Override
    public void destroy() throws IOException {
        log.info("MockDataSource() was destroyed.");
    }

}
