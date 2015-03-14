package com.budjb.mutex.exception

class LockNotAcquiredException extends Exception {
    LockNotAcquiredException() {
        super()
    }

    LockNotAcquiredException(String message) {
        super(message)
    }

    LockNotAcquiredException(String message, Throwable cause) {
        super(message, cause)
    }
}
