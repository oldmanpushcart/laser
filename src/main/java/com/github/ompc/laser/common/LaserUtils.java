package com.github.ompc.laser.common;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

import static java.lang.System.arraycopy;

/**
 * 业务工具类
 * Created by vlinux on 14-10-4.
 */
public final class LaserUtils {

    /**
     * 从size/3字符开始去掉size/3个字符，除法向下取整
     *
     * @param data 原数据
     * @return 处理后数据
     */
    public static byte[] process(byte[] data) {
        final int size = data.length;
        final int sub = size / 3;
        final byte[] newData = new byte[size - sub];
        arraycopy(data, 0, newData, 0, sub);
        arraycopy(data, sub + sub, newData, sub, newData.length - sub);
        return newData;
    }

    /**
     * 对字节数组进行逆序
     *
     * @param bricks 需要逆序的字节数组
     * @return 逆序后的字节数组
     */
    public static byte[] reverse(byte[] bricks) {
        if (null != bricks && bricks.length != 0) {
            byte temp;
            int len = bricks.length;
            for (int i = 0; i < bricks.length / 2; i++) {
                temp = bricks[i];
                bricks[i] = bricks[len - i - 1];
                bricks[len - i - 1] = temp;
            }
        }
        return bricks;
    }

    /**
     * 释放MappedByteBuffer
     *
     * @param buffer 需要被释放的映射缓存
     */
    public static void unmap(final MappedByteBuffer buffer) {
        if (buffer == null) {
            return;
        }
        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method getCleanerMethod = buffer.getClass().getMethod("cleaner");
                    if (getCleanerMethod != null) {
                        getCleanerMethod.setAccessible(true);
                        Object cleaner = getCleanerMethod.invoke(buffer);
                        Method cleanMethod = cleaner.getClass().getMethod("clean");
                        if (cleanMethod != null) {
                            cleanMethod.invoke(cleaner);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }

        });
    }

}
