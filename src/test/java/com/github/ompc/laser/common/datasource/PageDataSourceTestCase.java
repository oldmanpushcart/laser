package com.github.ompc.laser.common.datasource;

import com.github.ompc.laser.common.datasource.impl.PageDataSource;

import java.io.File;

/**
 * 阻塞型数据源测试用例
 * Created by vlinux on 14-9-21.
 */
public class PageDataSourceTestCase extends AbstractDataSourceTestCase {

    private DataSource currentDataSource;

    @Override
    DataSource getDataSource(boolean reset) {
        if (reset) {
            return currentDataSource = new PageDataSource(
                    new File("./src/test/resources/data/data_1000")
//                    new File("/Users/vlinux/data/data")
            );
        } else {
            return currentDataSource;
        }
    }

}
