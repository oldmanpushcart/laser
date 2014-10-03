package com.github.ompc.laser.server;

import java.io.File;

/**
 * Server端的配置
 * Created by vlinux on 14-9-29.
 */
public class ServerConfiger {

    /*
     * 监听端口
     */
    private int port;

    /*
     * 数据文件
     */
    private File dataFile;

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public File getDataFile() {
        return dataFile;
    }

    public void setDataFile(File dataFile) {
        this.dataFile = dataFile;
    }
}
