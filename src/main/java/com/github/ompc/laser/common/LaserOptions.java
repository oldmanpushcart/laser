package com.github.ompc.laser.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * laser's option config
 * Created by vlinux on 14-10-3.
 */
public final class LaserOptions {

    private boolean enableCompress = false;
    private int compressSize;

    private int clientSocketTimeout;
    private int clientSocketReceiverBufferSize;
    private int clientSocketSendBufferSize;
    private int clientReceiverBufferSize;
    private int clientSendBufferSize;
    private int clientSendCorkSize;
    private boolean clientSendAutoFlush;
    private boolean clientTcpNoDelay;
    private int clientTrafficClass;
    private int[] clientPerformancePreferences = new int[3];
    private int clientWorkNumbers;

    private int serverSocketTimeout;
    private int serverBacklog;


    private int serverChildSocketTimeout;
    private int serverChildSocketReceiverBufferSize;
    private int serverChildSocketSendBufferSize;
    private int serverChildReceiverBufferSize;
    private int serverChildSendCorkSize;
    private int serverChildSendBufferSize;
    private boolean serverSendAutoFlush;
    private boolean serverChildTcpNoDelay;
    private int serverChildTrafficClass;
    private int[] serverChildPerformancePreferences = new int[3];


    public LaserOptions(final File propertiesFile) throws IOException {

        final Properties properties = new Properties();
        properties.load(new FileInputStream(propertiesFile));

        enableCompress = Boolean.valueOf(properties.getProperty("enable_compress"));
        compressSize = Integer.valueOf(properties.getProperty("compress_size"));

        clientSocketTimeout = Integer.valueOf(properties.getProperty("client.socket_timeout"));

        clientSocketReceiverBufferSize = Integer.valueOf(properties.getProperty("client.socket_receiver_buffer_size"));
        clientSocketSendBufferSize = Integer.valueOf(properties.getProperty("client.socket_send_buffer_size"));

        clientReceiverBufferSize = Integer.valueOf(properties.getProperty("client.receiver_buffer_size"));
        clientSendBufferSize = Integer.valueOf(properties.getProperty("client.send_buffer_size"));
        clientSendCorkSize = Integer.valueOf(properties.getProperty("client.send_cork_size"));
        clientSendAutoFlush = Boolean.valueOf(properties.getProperty("client.send_auto_flush"));
        clientTcpNoDelay = Boolean.valueOf(properties.getProperty("client.tcp_no_delay"));
        clientTrafficClass = Integer.valueOf(properties.getProperty("client.traffic_class"));
        String[] clientPerformancePreferencesSplits = properties.getProperty("client.performance_preferences").split(",");
        clientPerformancePreferences[0] = Integer.valueOf(clientPerformancePreferencesSplits[0]);
        clientPerformancePreferences[1] = Integer.valueOf(clientPerformancePreferencesSplits[1]);
        clientPerformancePreferences[2] = Integer.valueOf(clientPerformancePreferencesSplits[2]);
        clientWorkNumbers = Integer.valueOf(properties.getProperty("client.work_numbers"));

        serverSocketTimeout = Integer.valueOf(properties.getProperty("server.socket_timeout"));
        serverBacklog = Integer.valueOf(properties.getProperty("server.backlog"));
        serverChildSocketTimeout = Integer.valueOf(properties.getProperty("server.child_socket_timeout"));
        serverChildSocketReceiverBufferSize = Integer.valueOf(properties.getProperty("server.child_socket_receiver_buffer_size"));
        serverChildSocketSendBufferSize = Integer.valueOf(properties.getProperty("server.child_socket_send_buffer_size"));
        serverChildReceiverBufferSize = Integer.valueOf(properties.getProperty("server.child_receiver_buffer_size"));
        serverChildSendBufferSize = Integer.valueOf(properties.getProperty("server.child_send_buffer_size"));
        serverChildSendCorkSize = Integer.valueOf(properties.getProperty("server.child_send_cork_size"));
        serverSendAutoFlush = Boolean.valueOf(properties.getProperty("server.child_send_auto_flush"));
        serverChildTcpNoDelay = Boolean.valueOf(properties.getProperty("server.child_tcp_no_delay"));
        serverChildTrafficClass = Integer.valueOf(properties.getProperty("server.child_traffic_class"));
        String[] serverChildPerformancePreferencesSplits = properties.getProperty("server.child_performance_preferences").split(",");
        serverChildPerformancePreferences[0] = Integer.valueOf(serverChildPerformancePreferencesSplits[0]);
        serverChildPerformancePreferences[1] = Integer.valueOf(serverChildPerformancePreferencesSplits[1]);
        serverChildPerformancePreferences[2] = Integer.valueOf(serverChildPerformancePreferencesSplits[2]);

    }

    public int getClientSocketTimeout() {
        return clientSocketTimeout;
    }

    public int getClientReceiverBufferSize() {
        return clientReceiverBufferSize;
    }

    public int getClientSendBufferSize() {
        return clientSendBufferSize;
    }

    public boolean isClientSendAutoFlush() {
        return clientSendAutoFlush;
    }

    public boolean isClientTcpNoDelay() {
        return clientTcpNoDelay;
    }

    public int getClientTrafficClass() {
        return clientTrafficClass;
    }

    public int[] getClientPerformancePreferences() {
        return clientPerformancePreferences;
    }

    public int getClientWorkNumbers() {
        return clientWorkNumbers;
    }

    public int getServerSocketTimeout() {
        return serverSocketTimeout;
    }

    public int getServerChildSocketTimeout() {
        return serverChildSocketTimeout;
    }

    public int getServerChildReceiverBufferSize() {
        return serverChildReceiverBufferSize;
    }

    public int getServerChildSendBufferSize() {
        return serverChildSendBufferSize;
    }

    public boolean isServerSendAutoFlush() {
        return serverSendAutoFlush;
    }

    public boolean isServerChildTcpNoDelay() {
        return serverChildTcpNoDelay;
    }

    public int getServerChildTrafficClass() {
        return serverChildTrafficClass;
    }

    public int[] getServerChildPerformancePreferences() {
        return serverChildPerformancePreferences;
    }

    public int getServerBacklog() {
        return serverBacklog;
    }

    public int getServerChildSendCorkSize() {
        return serverChildSendCorkSize;
    }

    public int getClientSendCorkSize() {
        return clientSendCorkSize;
    }

    public boolean isEnableCompress() {
        return enableCompress;
    }

    public int getClientSocketReceiverBufferSize() {
        return clientSocketReceiverBufferSize;
    }

    public int getClientSocketSendBufferSize() {
        return clientSocketSendBufferSize;
    }

    public int getServerChildSocketReceiverBufferSize() {
        return serverChildSocketReceiverBufferSize;
    }

    public int getServerChildSocketSendBufferSize() {
        return serverChildSocketSendBufferSize;
    }

    public int getCompressSize() {
        return compressSize;
    }
}
