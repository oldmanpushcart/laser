package com.github.ompc.laser.common.channel;

import com.github.ompc.laser.common.LaserUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * 实现GZIP压缩协议的WritableByteChannel
 * Created by vlinux on 14-10-9.
 */
public class GZIPWritableByteChannel implements WritableByteChannel {

    private final WritableByteChannel writableByteChannel;

    private final ByteBuffer compressBuffer;
    private final ByteBuffer unCompressBuffer;
//    private final byte[] unCompressData;

    private DecodeState state = DecodeState.READ;

    public GZIPWritableByteChannel(WritableByteChannel writableByteChannel, int size) {
        this.writableByteChannel = writableByteChannel;
        compressBuffer = ByteBuffer.allocate(size+Integer.BYTES);
        unCompressBuffer = ByteBuffer.allocate(size);
//        unCompressData = new byte[size];
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int count = 0;

        boolean hasMore = true;
        while (hasMore) {
            hasMore = false;

            switch (state) {

                case READ: {

                    if (!src.hasRemaining()) {
                        break;
                    }

                    while (src.hasRemaining()
                            && unCompressBuffer.hasRemaining()) {
                        unCompressBuffer.put(src.get());
                        count++;
                    }

                    if (!unCompressBuffer.hasRemaining()) {
                        unCompressBuffer.flip();
                        state = DecodeState.COMPRESS;
                    }
                    break;

                }

                case COMPRESS: {

                    final byte[] unCompressData = new byte[unCompressBuffer.limit()];
                    unCompressBuffer.get(unCompressData);
                    unCompressBuffer.compact();
                    final byte[] compressData = LaserUtils.compress(unCompressData, 1024);
                    compressBuffer.putInt(compressData.length);
                    compressBuffer.put(compressData);
                    compressBuffer.flip();
                    state = DecodeState.WRITE_DATA;

                }


                case WRITE_DATA: {

                    writableByteChannel.write(compressBuffer);
                    if (!compressBuffer.hasRemaining()) {
                        compressBuffer.compact();
                        state = DecodeState.READ;
                    }

                    hasMore = true;

                }

            }//switch:state

        }//while:hasMore

        return count;
    }

    @Override
    public boolean isOpen() {
        return writableByteChannel.isOpen();
    }

    @Override
    public void close() throws IOException {
        writableByteChannel.close();
    }

    enum DecodeState {
        READ,
        COMPRESS,
        WRITE_DATA
    }

}
