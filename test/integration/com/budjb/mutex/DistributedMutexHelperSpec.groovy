package com.budjb.mutex

import grails.plugin.spock.IntegrationSpec
import org.apache.log4j.Logger

class DistributedMutexHelperSpec extends IntegrationSpec {
    DistributedMutexHelper distributedMutexHelper

    def 'Test acquiring a mutex with only the identifier'() {
        setup:
        String id = UUID.randomUUID().toString()

        when:
        distributedMutexHelper.acquireMutexLock(id)

        then:
        DistributedMutex mutex = DistributedMutex.findByIdentifier(id)
        mutex != null
        mutex.locked == true
        mutex.expires == null
    }

    def 'Test acquiring a mutex with an identifier and a timeout'() {
        setup:
        String id = UUID.randomUUID().toString()

        when:
        distributedMutexHelper.acquireMutexLock(id, 10000)

        then:
        DistributedMutex mutex = DistributedMutex.findByIdentifier(id)
        mutex != null
        mutex.locked == true
        mutex.expires != null
    }

    def 'Test acquiring a mutex with a poll timeout while another process has a lock'() {
        setup:
        String id = UUID.randomUUID().toString()
        distributedMutexHelper.acquireMutexLock(id, 100)

        Logger oldLog = distributedMutexHelper.log
        Logger log = Mock(Logger)
        distributedMutexHelper.log = log

        when:
        boolean locked = distributedMutexHelper.acquireMutexLock(id, 10000, 1000)

        then:
        locked == true
        1 * log.warn("mutex identified by \"$id\" is expired and has been reacquired by a new requester")

        cleanup:
        distributedMutexHelper.log = oldLog
    }

    def 'If a mutex does not become available within the poll timeout period, the mutex is not acquired'() {
        setup:
        String id = UUID.randomUUID().toString()
        distributedMutexHelper.acquireMutexLock(id)

        when:
        boolean locked = distributedMutexHelper.acquireMutexLock(id, 10000, 100)

        then:
        locked == false
    }

    def 'Test releasing a mutex lock'() {
        setup:
        String id = UUID.randomUUID().toString()
        distributedMutexHelper.acquireMutexLock(id)

        when:
        distributedMutexHelper.releaseMutexLock(id)

        then:
        DistributedMutex distributedMutex = DistributedMutex.findByIdentifier(id)
        distributedMutex.locked == false
    }

    def 'Test checking whether a mutex is locked'() {
        setup:
        String id = UUID.randomUUID().toString()
        distributedMutexHelper.acquireMutexLock(id)

        when:
        boolean locked = distributedMutexHelper.isMutexLocked(id)

        then:
        locked == true

        when:
        distributedMutexHelper.releaseMutexLock(id)
        locked = distributedMutexHelper.isMutexLocked(id)

        then:
        locked == false
    }

    def 'If a mutex is acquired and released, it should be able to be acquired again'() {
        setup:
        String id = UUID.randomUUID().toString()
        distributedMutexHelper.acquireMutexLock(id)
        distributedMutexHelper.releaseMutexLock(id)

        when:
        boolean locked = distributedMutexHelper.acquireMutexLock(id)

        then:
        locked == true
        DistributedMutex distributedMutex = DistributedMutex.findByIdentifier(id)
        distributedMutex.locked == true
    }
}
