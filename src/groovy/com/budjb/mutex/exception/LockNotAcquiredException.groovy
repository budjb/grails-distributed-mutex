package com.budjb.mutex.exception

class LockNotAcquiredException extends RuntimeException {
    LockNotAcquiredException(String message) {
        super(message)
    }
}
