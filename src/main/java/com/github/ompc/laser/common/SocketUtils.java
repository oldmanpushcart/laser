package com.github.ompc.laser.common;

import com.github.ompc.laser.common.networking.GetDataReq;
import com.github.ompc.laser.common.networking.GetDataResp;
import com.github.ompc.laser.common.networking.GetEofResp;
import com.github.ompc.laser.common.networking.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log = LoggerFactory.getLogger(SocketUtils.class);

    /**
     * 格式化链接输出
     *
     * @param socket
     * @return
     */
    public static String format(Socket socket) {
        final StringBuilder sb = new StringBuilder("[");
        sb.append(socket.getLocalAddress()).append("->").append(socket.getRemoteSocketAddress());
        sb.append("]");
        return sb.toString();
    }

    /**
     * 读数据
     *
     * @param dis
     * @return
     * @throws IOException
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

                while (dis.available() >= len) {
                    // TODO : spin wait for timeout
                    log.info("spin for read, available={};len={}",dis.available(), len);
                }

                final byte[] data = new byte[len];
                dis.read(data);
                p = resp;
                break;
            case PRO_RESP_GETEOF:
                p = new GetEofResp();
                break;
        }

        return null;

    }

    /**
     * 写数据
     *
     * @param dos
     * @param p
     * @throws IOException
     */
    public static void write(DataOutputStream dos, Protocol p) throws IOException {

        dos.writeInt(p.getType());

        if (p instanceof GetDataResp) {
            final GetDataResp resp = (GetDataResp) p;
            dos.writeInt(resp.getLineNum());
            dos.writeInt(resp.getData().length);
            dos.write(resp.getData());
        }

        /*
        else if( p instanceof GetDataReq) {
            // do nothing...
        }

        else if( p instanceof GetEofResp) {
            // do nothing...
        }
        */

    }

}
