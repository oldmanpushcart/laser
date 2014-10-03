package com.github.ompc.laser.server.datasource;

import com.github.ompc.laser.server.datasource.impl.BlockDataSource;

import java.io.File;

/**
 * 阻塞型数据源测试用例
 * Created by vlinux on 14-9-21.
 */
public class BlockDataSourceTestCase extends AbstractDataSourceTestCase {

    private DataSource currentDataSource;

    @Override
    DataSource getDataSource(boolean reset) {
        if( reset ) {
            return currentDataSource = new BlockDataSource(
                    new File("./src/test/resources/data/data_1000")
//                    new File("/Users/vlinux/data/data")
            );
        } else {
            return currentDataSource;
        }
    }

}
