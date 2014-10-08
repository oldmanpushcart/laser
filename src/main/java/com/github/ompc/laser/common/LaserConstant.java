package com.github.ompc.laser.common;

/**
 * 常量类定义
 * Created by vlinux on 14-9-29.
 */
public class LaserConstant {

    /**
     * MC
     */
    public static final int PRO_MC = 0x0CFF;

    /**
     * MC掩码
     */
    public static final int PRO_MC_MASK = 0xFFFF0000;

    /**
     * 获取数据请求
     */
    public static final int PRO_REQ_GETDATA = PRO_MC << 16 | 0x01;

    /**
     * 返回数据请求
     */
    public static final int PRO_RESP_GETDATA = PRO_MC << 16 | 0x02;

    /**
     * 返回数据结束
     */
    public static final int PRO_RESP_GETEOF = PRO_MC << 16 | 0x03;

//    /**
//     * 返回压缩数据
//     */
//    public static final int PRO_RESP_GETCOMPRESS = PRO_MC << 16 | 0X04;

}
