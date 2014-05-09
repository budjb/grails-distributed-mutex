/*
 * Copyright 2014 Bud Byrd
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

import org.apache.log4j.Logger

import com.budjb.mutex.DistributedMutex
import com.budjb.mutex.DistributedMutexService

import groovy.time.TimeCategory

class DistributedMutexService {
    /**
     * Disable transactional behavior.
     */
    static transactional = false

    /**
     * Default poll interval (in milliseconds).
     */
    public static int DEFAULT_POLL_INTERVAL = 500

    /**
     * Default timeout (in milliseconds) to wait for a lock acquisition before giving up.
     */
    public static int DEFAULT_POLL_TIMEOUT = 0

    /**
     * Default mutex timeout (in milliseconds).
     */
    public static int DEFAULT_MUTEX_TIMEOUT = 0

    /**
     * Logger.
     */
    Logger log = Logger.getLogger(DistributedMutexService)

    /**
     * Determines if a mutex key is currently locked.
     *
     * @param key
     * @return
     */
    boolean isMutexLocked(String key) {
        // Load the mutex
        DistributedMutex mutex = DistributedMutex.findByKeyValue(key)

        // If there is no existing mutex, it cannot be locked
        if (!mutex) {
            return false
        }

        return isMutexLocked(mutex)
    }

    /**
     * Determines if a mutex key is currently locked.
     *
     * @param mutex
     * @return
     */
    boolean isMutexLocked(DistributedMutex mutex) {
        // Check if the mutex is unlocked
        if (!mutex.locked) {
            return false
        }

        // If the mutex is locked, and doesn't expire, keep the lock
        if (mutex.expires == null) {
            return true
        }

        // Finally, check whether the lock has expired
        if (mutex.expires.time >= new Date().time) {
            return true
        }

        return false
    }

    /**
     * Attempts to acquire a mutex lock for a given key.
     *
     * @param key Key value that identifies the mutex.
     * @return
     */
    boolean acquireMutexLock(String key) {
        return acquireMutexLock(key, DEFAULT_MUTEX_TIMEOUT)
    }

    /**
     * Attempts to acquire a mutex lock for a given key.
     *
     * @param key Key value that identifies the mutex.
     * @param mutexTimeout Amount of time the mutex lock is valid until it can be reacquired.
     * @return
     */
    boolean acquireMutexLock(String key, long mutexTimeout) {
        return acquireMutexLock(key, mutexTimeout, DEFAULT_POLL_TIMEOUT)
    }

    /**
     * Attempts to acquire a mutex lock for a given key.
     *
     * @param key Key value that identifies the mutex.
     * @param mutexTimeout Amount of time the mutex lock is valid until it can be reacquired.
     * @param pollTimeout Amount of time to wait for the mutex to become available.
     * @return
     */
    boolean acquireMutexLock(String key, long mutexTimeout, long pollTimeout) {
        return acquireMutexLock(key, mutexTimeout, pollTimeout, DEFAULT_POLL_INTERVAL)
    }

    /**
     * Attempts to acquire a mutex lock for a given key.
     *
     * @param key Key value that identifies the mutex.
     * @param mutexTimeout Amount of time the mutex lock is valid until it can be reacquired.
     * @param pollTimeout Amount of time to wait for the mutex to become available.
     * @param pollInterval Amount of time to wait before checking on the mutex availability.
     * @return
     */
    boolean acquireMutexLock(String key, long mutexTimeout, long pollTimeout, long pollInterval) {
        // Mark the start time
        Long start = new Date().time

        // Loop until the timeout is reached
        while (pollTimeout == 0 || new Date().time < start + pollTimeout) {
            try {
                // Track success
                boolean locked = false

                // Run within a transaction
                DistributedMutex.withTransaction {
                    // Find the existing mutex
                    DistributedMutex mutex = DistributedMutex.findByKeyValue(key)

                    // If the mutex wasn't found, create one. Otherwise, force a refresh.
                    if (!mutex) {
                        mutex = new DistributedMutex(keyValue: key)
                    }
                    else {
                        mutex.refresh()
                    }

                    // Check if the mutex is locked
                    if (isMutexLocked(mutex)) {
                        return
                    }

                    // Log a warning if the mutex is expired
                    if (mutex.expires && mutex.expires.time < new Date().time) {
                        log.warn("mutex identified by \"${key}\" is expired and has been reacquired by a new requester")
                    }

                    // Determine the expiration time
                    Date expires = null
                    if (mutexTimeout > 0) {
                        use(TimeCategory) {
                            expires = new Date() + (mutexTimeout as int).milliseconds
                        }
                    }

                    // Mark the mutex locked and save
                    mutex.locked = true
                    mutex.expires = expires
                    mutex.save(flush: true, failOnError: true)

                    // Mark it locked
                    locked = true
                }

                // Return now if the lock was acquired
                if (locked) {
                    return true
                }
            }
            catch (Exception e) {
                log.debug("exception caught while attempting to acquire lock; will automatically retry if the acquire timeout has not expired", e)
            }

            // If timeout is 0, just stop at one try
            if (pollTimeout == 0) {
                return false
            }

            // Sleep a bit
            sleep pollInterval
        }

        return false
    }

    /**
     * Releases a mutex lock for a given key.
     *
     * @param key
     */
    void releaseMutexLock(String key) {
        DistributedMutex.withTransaction {
            // Find the mutex
            DistributedMutex mutex = DistributedMutex.findByKeyValue(key)

            // If one wasn't found, quit
            if (!mutex) {
                log.warn("no mutex with key \"${key}\" was found")
                return
            }

            // Mark it unlocked and save
            mutex.locked = false
            mutex.expires = null
            mutex.save(flush: true, failOnError: true)
        }
    }
}
