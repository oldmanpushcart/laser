package com.github.ompc.laser.common;

import java.net.Socket;

/**
 * Socket工具类
 * Created by vlinux on 14-10-3.
 */
public class SocketUtils {

    /**
     * 格式化链接输出
     *
     * @param socket socket链接
     * @return 格式化后的信息
     */
    public static String format(Socket socket) {
        return "["
                + socket.getLocalAddress()
                + "->"
                + socket.getRemoteSocketAddress()
                + "]";
    }

}
