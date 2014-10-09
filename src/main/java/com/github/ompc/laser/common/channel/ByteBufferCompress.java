package com.github.ompc.laser.common.channel;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * ByteBuffer压缩
 * Created by vlinux on 14-10-9.
 */
public interface ByteBufferCompress {

    void compress(ByteBuffer src, int len, ByteBuffer dst) throws IOException;
    void unCompress(ByteBuffer src, int len, ByteBuffer dst) throws IOException;

}
