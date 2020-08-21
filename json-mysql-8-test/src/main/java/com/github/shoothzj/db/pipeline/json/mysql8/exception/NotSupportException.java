package com.github.shoothzj.db.pipeline.json.mysql8.exception;

/**
 * @author hezhangjian
 */
public class NotSupportException extends IllegalArgumentException{

    public NotSupportException() {
        super();
    }

    public NotSupportException(String s) {
        super(s);
    }

    public NotSupportException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotSupportException(Throwable cause) {
        super(cause);
    }
}
