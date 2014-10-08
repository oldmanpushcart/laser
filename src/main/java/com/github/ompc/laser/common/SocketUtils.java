package com.github.ompc.laser.common;

import com.github.ompc.laser.common.networking.GetDataReq;
import com.github.ompc.laser.common.networking.GetDataResp;
import com.github.ompc.laser.common.networking.GetEofResp;
import com.github.ompc.laser.common.networking.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import static com.github.ompc.laser.common.LaserConstant.*;

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

    /**
     * 读数据
     *
     * @param dis 数据流
     * @return 解析后的协议
     * @throws IOException 网络出错
     */
    public static Protocol read(DataInputStream dis) throws IOException {
        final int type = dis.readInt();

        // check MC
        if ((type & PRO_MC_MASK) >> 16 != PRO_MC) {
            throw new IOException("illegal type=" + type);
        }

        final Protocol p;
        switch (type) {
            case PRO_REQ_GETDATA:
                p = new GetDataReq();
                break;
            case PRO_RESP_GETDATA:
                final GetDataResp resp = new GetDataResp();
                resp.setLineNum(dis.readInt());
                final int len = dis.readInt();

                while (dis.available() < len) {

                }

                final byte[] data = new byte[len];
                dis.read(data);
                resp.setData(data);

                p = resp;
                break;
            case PRO_RESP_GETEOF:
                p = new GetEofResp();
                break;
            default:
                throw new IOException("illegal type=" + type);
        }

        return p;

    }

    /**
     * 写数据
     *
     * @param dos 数据输出流
     * @param p 协议报文
     * @throws IOException 网络出错
     */
    public static void write(DataOutputStream dos, Protocol p) throws IOException {

        dos.writeInt(p.getType());

        if (p instanceof GetDataResp) {
            final GetDataResp resp = (GetDataResp) p;
            dos.writeInt(resp.getLineNum());
            dos.writeInt(resp.getData().length);
            dos.write(resp.getData());
        }

    }

}
