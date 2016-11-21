/*
 * Copyright 2015-2016 Bud Byrd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.budjb.mutex

import com.budjb.mutex.exception.LockNotAcquiredException
import grails.test.mixin.integration.Integration
import grails.transaction.Rollback
import org.slf4j.Logger
import spock.lang.Specification

@Integration
@Rollback
class DistributedMutexHelperSpec extends Specification {
    final static String MUTEX_IDENTIFIER_NAME = 'foo-bar'

    DistributedMutexHelper distributedMutexHelper

    def 'Test acquiring a mutex with only the identifier'() {
        when:
        String key = distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME)

        then:
        notThrown LockNotAcquiredException

        key != null

        when:
        DistributedMutex mutex = DistributedMutex.findByIdentifier(MUTEX_IDENTIFIER_NAME)

        then:
        mutex != null
        mutex.locked
        mutex.expires == null
        mutex.key == key
    }

    def 'Test acquiring a mutex with an identifier and a timeout'() {
        when:
        String key = distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME, 10000)

        then:
        notThrown LockNotAcquiredException

        key != null

        when:
        DistributedMutex mutex = DistributedMutex.findByIdentifier(MUTEX_IDENTIFIER_NAME)

        then:
        mutex != null
        mutex.locked
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
        locked
        1 * log.warn('mutex identified by \'{}\' is expired and has been reacquired by a new requester', 'foo-bar')

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
        DistributedMutex distributedMutex = DistributedMutex.findByIdentifier(MUTEX_IDENTIFIER_NAME)

        then:
        !distributedMutex.locked
    }

    def 'Test releasing a mutex lock with the incorrect key'() {
        setup:
        Logger log = Mock(Logger)
        distributedMutexHelper.log = log

        distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME)

        when:
        distributedMutexHelper.releaseMutexLock(MUTEX_IDENTIFIER_NAME, 'foobar')
        DistributedMutex distributedMutex = DistributedMutex.findByIdentifier(MUTEX_IDENTIFIER_NAME)

        then:
        distributedMutex.locked
        1 * log.warn('the key on mutex with identifier \'{}\' does not match the key provided with the request to unlock the mutex', 'foo-bar')
    }

    def 'Test checking whether a mutex is locked'() {
        setup:
        String key = distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME)

        when:
        boolean locked = distributedMutexHelper.isMutexLocked(MUTEX_IDENTIFIER_NAME)

        then:
        notThrown LockNotAcquiredException
        locked

        when:
        distributedMutexHelper.releaseMutexLock(MUTEX_IDENTIFIER_NAME, key)
        locked = distributedMutexHelper.isMutexLocked(MUTEX_IDENTIFIER_NAME)

        then:
        !locked
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

        when:
        DistributedMutex distributedMutex = DistributedMutex.findByIdentifier(MUTEX_IDENTIFIER_NAME)

        then:
        distributedMutex.locked
    }

    def 'When a mutex is locked, forcibly releasing it allows it to be reacquired'() {
        setup:
        distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME)

        expect:
        distributedMutexHelper.isMutexLocked(MUTEX_IDENTIFIER_NAME)

        when:
        distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME, DistributedMutexHelper.DEFAULT_MUTEX_TIMEOUT, 100)

        then:
        thrown LockNotAcquiredException

        when:
        distributedMutexHelper.forciblyReleaseMutex(MUTEX_IDENTIFIER_NAME)
        distributedMutexHelper.acquireMutexLock(MUTEX_IDENTIFIER_NAME)

        then:
        notThrown LockNotAcquiredException
    }

    def 'If a mutex has never been used, then a lock check should be negative'() {
        when:
        boolean isLocked = distributedMutexHelper.isMutexLocked(MUTEX_IDENTIFIER_NAME)

        then: 'No exceptions are thrown, and the mutex is not locked'
        !isLocked
    }
}
