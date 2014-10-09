package com.github.ompc.laser.common.datasource;

/**
 * 一行数据
 * Created by vlinux on 14-9-21.
 */
public class Row {

    /*
     * 行号
     */
    private int lineNum;

    /*
     * 数据内容
     */
    private byte[] data;

    public Row() {
        //
    }

    public Row(int lineNum, byte[] data) {
        this.lineNum = lineNum;
        this.data = data;
    }

    public int getLineNum() {
        return lineNum;
    }

    public void setLineNum(int lineNum) {
        this.lineNum = lineNum;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
