package com.github.ompc.laser.server.datasource;

import com.github.ompc.laser.server.datasource.impl.MappingDataSource;

import java.io.File;

/**
 * 内存映射型数据源测试用例
 * Created by vlinux on 14-10-4.
 */
public class MappingDataSourceTestCase extends AbstractDataSourceTestCase {

    private DataSource currentDataSource;

    @Override
    DataSource getDataSource(boolean reset) {
        if (reset) {
            return currentDataSource = new MappingDataSource(
                    new File("./src/test/resources/data/data_1000")
//                    new File("/Users/vlinux/data/data")
            );
        } else {
            return currentDataSource;
        }
    }
}
