package com.github.houbb.mq.common.resp;

import com.github.houbb.heaven.response.respcode.RespCode;

/**
 * @author binbin.hou
 * @since 1.0.0
 */
public class MqException extends RuntimeException {

    private final RespCode respCode;

    public MqException(RespCode respCode) {
        this.respCode = respCode;
    }

    public MqException(String message, RespCode respCode) {
        super(message);
        this.respCode = respCode;
    }

    public MqException(String message, Throwable cause, RespCode respCode) {
        super(message, cause);
        this.respCode = respCode;
    }

    public MqException(Throwable cause, RespCode respCode) {
        super(cause);
        this.respCode = respCode;
    }

    public MqException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, RespCode respCode) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.respCode = respCode;
    }
}