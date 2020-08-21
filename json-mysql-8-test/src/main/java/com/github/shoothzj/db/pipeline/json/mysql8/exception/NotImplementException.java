package com.github.shoothzj.db.pipeline.json.mysql8.exception;

/**
 * @author hezhangjian
 */
public class NotImplementException extends IllegalArgumentException {

    public NotImplementException() {
        super();
    }

    public NotImplementException(String s) {
        super(s);
    }

    public NotImplementException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotImplementException(Throwable cause) {
        super(cause);
    }
}
