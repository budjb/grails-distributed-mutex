package com.budjb.mutex

import com.budjb.mutex.exception.LockNotAcquiredException
import grails.plugin.spock.IntegrationSpec
import org.apache.log4j.Logger

import java.util.concurrent.locks.Lock

class DistributedMutexHelperSpec extends IntegrationSpec {
    final static String MUTEX_IDENTIFIER_NAME = 'foo-bar'

    DistributedMutexHelper distributedMutexHelper

    def 'Test acquiring a mutex with only the identifier'() {
        when:
        String key = distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME)

        then:
        notThrown LockNotAcquiredException

        key != null

        DistributedMutex mutex = DistributedMutex.findByIdentifier(MUTEX_IDENTIFIER_NAME)
        mutex != null
        mutex.locked == true
        mutex.expires == null
        mutex.key == key
    }

    def 'Test acquiring a mutex with an identifier and a timeout'() {
        when:
        String key = distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME, 10000)

        then:
        notThrown LockNotAcquiredException

        key != null

        DistributedMutex mutex = DistributedMutex.findByIdentifier(MUTEX_IDENTIFIER_NAME)
        mutex != null
        mutex.locked == true
        mutex.expires != null
        mutex.key == key
    }

    def 'Test acquiring a mutex with a poll timeout while another process has a lock'() {
        setup:
        distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME, 100)

        Logger oldLog = distributedMutexHelper.log
        Logger log = Mock(Logger)
        distributedMutexHelper.log = log

        when:
        boolean locked = distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME, 10000, 1000)

        then:
        locked == true
        1 * log.warn("mutex identified by '$MUTEX_IDENTIFIER_NAME' is expired and has been reacquired by a new requester")

        cleanup:
        distributedMutexHelper.log = oldLog
    }

    def 'If a mutex does not become available within the poll timeout period, the mutex is not acquired'() {
        setup:
        distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME)

        when:
        distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME, 10000, 100)

        then:
        thrown LockNotAcquiredException
    }

    def 'Test releasing a mutex lock'() {
        setup:
        String key = distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME)

        when:
        distributedMutexHelper.releaseMutexLock(MUTEX_IDENTIFIER_NAME, key)

        then:
        DistributedMutex distributedMutex = DistributedMutex.findByIdentifier(MUTEX_IDENTIFIER_NAME)
        distributedMutex.locked == false
    }

    def 'Test checking whether a mutex is locked'() {
        setup:
        String key = distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME)

        when:
        boolean locked = distributedMutexHelper.isMutexLocked(MUTEX_IDENTIFIER_NAME)

        then:
        notThrown LockNotAcquiredException

        when:
        distributedMutexHelper.releaseMutexLock(MUTEX_IDENTIFIER_NAME, key)
        locked = distributedMutexHelper.isMutexLocked(MUTEX_IDENTIFIER_NAME)

        then:
        locked == false
    }

    def 'If a mutex is acquired and released, it should be able to be acquired again'() {
        setup:
        String key = distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME)
        distributedMutexHelper.releaseMutexLock(MUTEX_IDENTIFIER_NAME, key)

        when:
        key = distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME)

        then:
        notThrown LockNotAcquiredException
        key != null
        DistributedMutex distributedMutex = DistributedMutex.findByIdentifier(MUTEX_IDENTIFIER_NAME)
        distributedMutex.locked == true
    }
}
