package com.github.ompc.laser.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * laser's option config
 * Created by vlinux on 14-10-3.
 */
public class LaserOptions {

    private int clientSocketTimeout;
    private int clientReceiverBufferSize;
    private int clientSendBufferSize;
    private boolean clientTcpNoDelay;
    private int clientTrafficClass;
    private int[] clientPerformancePreferences;
    private int clientWorkNumbers;

    private int serverSocketTimeout;
    private int serverChildSocketTimeout;
    private int serverChildReceiverBufferSize;
    private int serverChildSendBufferSize;
    private boolean serverChildTcpNoDelay;
    private int serverChildTrafficClass;
    private int[] serverChildPerformancePreferences;


    public LaserOptions(final File propertiesFile) throws IOException {

        final Properties properties = new Properties();
        properties.load(new FileInputStream(propertiesFile));

        clientSocketTimeout = Integer.valueOf(properties.getProperty("client.socket_timeout"));
        clientReceiverBufferSize = Integer.valueOf(properties.getProperty("client.receiver_buffer_size"));
        clientSendBufferSize = Integer.valueOf(properties.getProperty("client.send_buffer_size"));
        clientTcpNoDelay = Boolean.valueOf(properties.getProperty("client.tcp_no_delay"));
        clientTrafficClass = Integer.valueOf(properties.getProperty("client.traffic_class"));
        String[] clientPerformancePreferencesSplits = properties.getProperty("client.performance_preferences").split(",");
        clientPerformancePreferences[0] = Integer.valueOf(clientPerformancePreferencesSplits[0]);
        clientPerformancePreferences[1] = Integer.valueOf(clientPerformancePreferencesSplits[1]);
        clientPerformancePreferences[2] = Integer.valueOf(clientPerformancePreferencesSplits[2]);
        clientWorkNumbers = Integer.valueOf(properties.getProperty("client.work_numbers"));

        serverSocketTimeout = Integer.valueOf(properties.getProperty("server.socket_timeout"));
        serverChildSocketTimeout = Integer.valueOf(properties.getProperty("server.child_socket_timeout"));
        serverChildReceiverBufferSize = Integer.valueOf(properties.getProperty("server.child_receiver_buffer_size"));
        serverChildSendBufferSize = Integer.valueOf(properties.getProperty("server.child_send_buffer_size"));
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

    public void setClientSocketTimeout(int clientSocketTimeout) {
        this.clientSocketTimeout = clientSocketTimeout;
    }

    public int getClientReceiverBufferSize() {
        return clientReceiverBufferSize;
    }

    public void setClientReceiverBufferSize(int clientReceiverBufferSize) {
        this.clientReceiverBufferSize = clientReceiverBufferSize;
    }

    public int getClientSendBufferSize() {
        return clientSendBufferSize;
    }

    public void setClientSendBufferSize(int clientSendBufferSize) {
        this.clientSendBufferSize = clientSendBufferSize;
    }

    public boolean isClientTcpNoDelay() {
        return clientTcpNoDelay;
    }

    public void setClientTcpNoDelay(boolean clientTcpNoDelay) {
        this.clientTcpNoDelay = clientTcpNoDelay;
    }

    public int getClientTrafficClass() {
        return clientTrafficClass;
    }

    public void setClientTrafficClass(int clientTrafficClass) {
        this.clientTrafficClass = clientTrafficClass;
    }

    public int[] getClientPerformancePreferences() {
        return clientPerformancePreferences;
    }

    public void setClientPerformancePreferences(int[] clientPerformancePreferences) {
        this.clientPerformancePreferences = clientPerformancePreferences;
    }

    public int getClientWorkNumbers() {
        return clientWorkNumbers;
    }

    public void setClientWorkNumbers(int clientWorkNumbers) {
        this.clientWorkNumbers = clientWorkNumbers;
    }

    public int getServerSocketTimeout() {
        return serverSocketTimeout;
    }

    public void setServerSocketTimeout(int serverSocketTimeout) {
        this.serverSocketTimeout = serverSocketTimeout;
    }

    public int getServerChildSocketTimeout() {
        return serverChildSocketTimeout;
    }

    public void setServerChildSocketTimeout(int serverChildSocketTimeout) {
        this.serverChildSocketTimeout = serverChildSocketTimeout;
    }

    public int getServerChildReceiverBufferSize() {
        return serverChildReceiverBufferSize;
    }

    public void setServerChildReceiverBufferSize(int serverChildReceiverBufferSize) {
        this.serverChildReceiverBufferSize = serverChildReceiverBufferSize;
    }

    public int getServerChildSendBufferSize() {
        return serverChildSendBufferSize;
    }

    public void setServerChildSendBufferSize(int serverChildSendBufferSize) {
        this.serverChildSendBufferSize = serverChildSendBufferSize;
    }

    public boolean isServerChildTcpNoDelay() {
        return serverChildTcpNoDelay;
    }

    public void setServerChildTcpNoDelay(boolean serverChildTcpNoDelay) {
        this.serverChildTcpNoDelay = serverChildTcpNoDelay;
    }

    public int getServerChildTrafficClass() {
        return serverChildTrafficClass;
    }

    public void setServerChildTrafficClass(int serverChildTrafficClass) {
        this.serverChildTrafficClass = serverChildTrafficClass;
    }

    public int[] getServerChildPerformancePreferences() {
        return serverChildPerformancePreferences;
    }

    public void setServerChildPerformancePreferences(int[] serverChildPerformancePreferences) {
        this.serverChildPerformancePreferences = serverChildPerformancePreferences;
    }
}
