package com.github.ompc.laser.common.datasource;

import java.io.IOException;

/**
 * 数据持久化
 * Created by vlinux on 14-10-4.
 */
public interface DataPersistence {


    /**
     * 保存一行数据
     *
     * @param row
     * @throws IOException
     */
    void putRow(Row row) throws IOException;

    /**
     * 初始化数据持久化
     *
     * @throws IOException 数据持久化初始化失败
     */
    void init() throws IOException;

    /**
     * 刷新持久化数据
     *
     * @throws IOException 刷入磁盘失败
     */
    void flush() throws IOException;

    /**
     * 销毁数据持久化
     *
     * @throws IOException 数据持久化销毁失败
     */
    void destroy() throws IOException;


}
