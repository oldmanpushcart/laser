package com.github.ompc.laser.common.datasource;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.Runtime.getRuntime;

/**
 * 数据源测试用例
 * Created by vlinux on 14-9-21.
 */
public abstract class AbstractDataSourceTestCase {

    /**
     * 获取数据源
     *
     * @param reset 是否重置数据源
     * @return 返回数据源实现
     */
    abstract DataSource getDataSource(boolean reset);

    @Before
    public void initDataSource() throws IOException {
        getDataSource(true).init();
    }

    @After
    public void destroyDataSource() throws IOException {
        getDataSource(false).destroy();
    }

    /**
     * 单线程获取数据
     *
     * @throws Exception
     */
    @Test
    public void testGetRowBySingleThread() throws Exception {

        final DataSource dataSource = getDataSource(false);
        // 头1000行正常获取
        for (int index = 0; index < 1000; index++) {
            final Row row = dataSource.getRow(new Row());
            Assert.assertEquals(row.getLineNum(), index);
            Assert.assertTrue(row.getData().length > 0);
        }

        // 第1001行到达文件末尾需要Row的行号为-1
        final Row row = dataSource.getRow(new Row());
        Assert.assertTrue(row.getLineNum() < 0);

    }

    /**
     * 多线程获取数据
     *
     * @throws Exception
     */
    @Test
    public void testGetRowByMultiThread() throws Exception {

        final DataSource dataSource = getDataSource(false);
        final Set<Integer> unique = new ConcurrentSkipListSet<>();
        final ExecutorService executors = Executors.newCachedThreadPool();

        try {
            final Future<Integer> resultFuture = executors.submit(() -> {

                final Future<Integer>[] forks = new Future[getRuntime().availableProcessors()];
                for (int i = 0; i < forks.length; i++) {
                    forks[i] = executors.submit(() -> {
                        int counter = 0;
                        while (true) {
                            final Row row = dataSource.getRow(new Row());
                            if (row.getLineNum() >= 0
                                    && row.getData().length > 0
                                    && !unique.contains(row.getLineNum())) {
                                counter++;
                                unique.add(row.getLineNum());
                            }//if

                            if (row.getLineNum() < 0) {
                                break;
                            }
                        }
                        return counter;
                    });
                }

                int counter = 0;
                for (Future<Integer> forkFuture : forks) {
                    counter += forkFuture.get();
                }
                return counter;

            });
            Assert.assertEquals(resultFuture.get().intValue(), 1000);
        } finally {
            executors.shutdown();
        }

    }

}
