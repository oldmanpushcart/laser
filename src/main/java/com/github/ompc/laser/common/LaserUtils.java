package com.github.ompc.laser.common;

import static java.lang.System.arraycopy;

/**
 * 业务工具类
 * Created by vlinux on 14-10-4.
 */
public class LaserUtils {

    /**
     * 从size/3字符开始去掉size/3个字符，除法向下取整
     * @param data 原数据
     * @return 处理后数据
     */
    public static byte[] process(byte[] data) {
        final int size = data.length;
        final int sub = size / 3;
        final byte[] newData = new byte[size - sub];
        arraycopy(data, 0, newData, 0, sub);
        arraycopy(data, sub+sub, newData, sub, newData.length-sub);
        return newData;
    }

}
