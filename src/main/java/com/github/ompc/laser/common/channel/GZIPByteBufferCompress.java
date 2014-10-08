package com.github.ompc.laser.common.channel;

import com.github.ompc.laser.common.LaserUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by vlinux on 14-10-9.
 */
public class GZIPByteBufferCompress implements ByteBufferCompress {

    @Override
    public void compress(ByteBuffer src, int len, ByteBuffer dst) throws IOException {
        final byte[] unCompressData = new byte[len];
        src.get(unCompressData);
        final byte[] compressData = LaserUtils.compress(unCompressData, 1024);
        dst.putInt(compressData.length);
        dst.put(compressData);
    }

    @Override
    public void unCompress(ByteBuffer src, int len, ByteBuffer dst) throws IOException {
        final byte[] compressData = new byte[len];
        src.get(compressData);
        final byte[] unCompressData = LaserUtils.unCompress(compressData, 1024);
        dst.put(unCompressData);
    }

}
