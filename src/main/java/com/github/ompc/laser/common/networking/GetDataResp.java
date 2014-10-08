package com.github.ompc.laser.common.networking;

import static com.github.ompc.laser.common.LaserConstant.PRO_RESP_GETDATA;

/**
 * 应答报文
 * Created by vlinux on 14-9-29.
 * @deprecated 不再使用
 */
public final class GetDataResp extends Protocol {

    /*
     * 行号
     */
    private int lineNum;

    /*
     * 数据内容
     */
    private byte[] data;

    public GetDataResp() {
        super(PRO_RESP_GETDATA);
    }

    public GetDataResp(int lineNum, byte[] data) {
        this();
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
