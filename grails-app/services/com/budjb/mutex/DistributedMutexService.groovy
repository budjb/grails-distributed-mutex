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

import com.budjb.mutex.DistributedMutexService;
import com.rackspace.rvi.mutex.DistributedMutex;

class DistributedMutexService {
    /**
     * Disable transactional behavior.
     */
    static transactional = false

    /**
     * Default poll interval (in milliseconds).
     */
    private static int DEFAULT_POLL_INTERVAL = 500

    /**
     * Default timeout to wait for a lock acquisition before giving up.
     */
    private static int DEFAULT_TIMEOUT = 0

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
        return DistributedMutex.findByKeyValue(key)?.locked ?: false
    }

    /**
     * Attempts to acquire a mutex lock for a given key.
     *
     * @param key Key value that identifies the mutex.
     * @return
     */
    boolean acquireMutexLock(String key) {
        return acquireMutexLock(key, DEFAULT_TIMEOUT)
    }

    /**
     * Attempts to acquire a mutex lock for a given key.
     *
     * @param key Key value that identifies the mutex.
     * @param timeout Amount of time to wait for the mutex to become available.
     * @return
     */
    boolean acquireMutexLock(String key, long timeout) {
        return acquireMutexLock(key, timeout, DEFAULT_POLL_INTERVAL)
    }

    /**
     * Attempts to acquire a mutex lock for a given key.
     *
     * @param key Key value that identifies the mutex.
     * @param timeout Amount of time to wait for the mutex to become available.
     * @param poll Amount of time to wait before checking on the mutex availability.
     * @return
     */
    boolean acquireMutexLock(String key, long timeout, long poll) {
        // Mark the start time
        Long start = new Date().time

        // Loop until the timeout is reached
        while (timeout == 0 || new Date().time < start + timeout) {
            try {
                // Track success
                boolean locked = false

                // Run within a transaction
                DistributedMutex.withTransaction {
                    // Find the existing mutex
                    DistributedMutex mutex = DistributedMutex.findByKeyValue(key)

                    // If the mutex wasn't found, create one
                    if (!mutex) {
                        mutex = new DistributedMutex(keyValue: key)
                    }

                    // Force a refresh
                    mutex.refresh()

                    // If the mutex is locked, we're done
                    if (mutex.locked) {
                        return
                    }

                    // Mark the mutex locked and save
                    mutex.locked = true
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
            if (timeout == 0) {
                return false
            }

            // Sleep a bit
            sleep poll
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
            mutex.save(flush: true, failOnError: true)
        }
    }
}
