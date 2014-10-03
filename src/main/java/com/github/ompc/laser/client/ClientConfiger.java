package com.github.ompc.laser.client;

import java.io.File;
import java.net.InetSocketAddress;

/**
 * 客户端配置
 * Created by vlinux on 14-9-30.
 */
public class ClientConfiger {

    private File dataFile;
    private InetSocketAddress serverAddress;

    public InetSocketAddress getServerAddress() {
        return serverAddress;
    }

    public void setServerAddress(InetSocketAddress serverAddress) {
        this.serverAddress = serverAddress;
    }

    public File getDataFile() {
        return dataFile;
    }

    public void setDataFile(File dataFile) {
        this.dataFile = dataFile;
    }
}
