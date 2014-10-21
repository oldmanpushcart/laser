package com.github.ompc.laser.common.datasource;

import java.io.IOException;

/**
 * 数据源接口
 * Created by vlinux on 14-9-21.
 */
public interface DataSource {

    /**
     * 获取一行数据
     *
     * @param row 希望被填充的行
     * @return 返回一行数据
     * @throws IOException 若文件访问失败，则抛出IOException
     *                     若到达文件末端，则抛出EOFException
     */
    Row getRow(Row row) throws IOException;

    /**
     * 初始化数据源
     *
     * @throws IOException 数据源初始化失败
     */
    void init() throws IOException;

    /**
     * 销毁数据源
     *
     * @throws IOException 数据源销毁失败
     */
    void destroy() throws IOException;

}
