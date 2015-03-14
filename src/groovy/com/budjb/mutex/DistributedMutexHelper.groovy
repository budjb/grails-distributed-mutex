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
import org.hibernate.StaleObjectStateException
import org.springframework.dao.OptimisticLockingFailureException
import groovy.time.TimeCategory

class DistributedMutexHelper {
    /**
     * Default poll interval (in milliseconds).
     */
    static final int DEFAULT_POLL_INTERVAL = 500

    /**
     * Default timeout (in milliseconds) to wait for a lock acquisition before giving up.
     */
    static final int DEFAULT_POLL_TIMEOUT = 0

    /**
     * Default mutex timeout (in milliseconds).
     */
    static final int DEFAULT_MUTEX_TIMEOUT = 0

    /**
     * Logger.
     */
    Logger log = Logger.getLogger(DistributedMutexHelper)

    /**
     * Determines if a mutex is currently locked.
     *
     * @param identifier
     * @return
     */
    boolean isMutexLocked(String identifier) {
        // Load the mutex
        DistributedMutex mutex
        DistributedMutex.withNewSession {
            mutex = DistributedMutex.findByIdentifier(identifier)
        }

        // If there is no existing mutex, it cannot be locked
        if (!mutex) {
            return false
        }

        return isMutexLocked(mutex)
    }

    /**
     * Determines if a mutex is currently locked.
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
     * Attempts to acquire a mutex lock for a given identifier.
     *
     * @param identifier Identity of the mutex.
     * @return
     */
    boolean acquireMutexLock(String identifier) {
        return acquireMutexLock(identifier, DEFAULT_MUTEX_TIMEOUT)
    }

    /**
     * Attempts to acquire a mutex lock for a given identifier.
     *
     * @param identifier Identity of the mutex.
     * @param mutexTimeout Amount of time the mutex lock is valid until it can be reacquired.
     * @return
     */
    boolean acquireMutexLock(String identifier, long mutexTimeout) {
        return acquireMutexLock(identifier, mutexTimeout, DEFAULT_POLL_TIMEOUT)
    }

    /**
     * Attempts to acquire a mutex lock for a given identifier.
     *
     * @param identifier Identifier of the mutex.
     * @param mutexTimeout Amount of time the mutex lock is valid until it can be reacquired.
     * @param pollTimeout Amount of time to wait for the mutex to become available.
     * @return
     */
    boolean acquireMutexLock(String identifier, long mutexTimeout, long pollTimeout) {
        return acquireMutexLock(identifier, mutexTimeout, pollTimeout, DEFAULT_POLL_INTERVAL)
    }

    /**
     * Attempts to acquire a mutex lock for a given identifier.
     *
     * @param identifier Identifier of the mutex.
     * @param mutexTimeout Amount of time the mutex lock is valid until it can be reacquired.
     * @param pollTimeout Amount of time to wait for the mutex to become available.
     * @param pollInterval Amount of time to wait before checking on the mutex availability.
     * @return
     */
    boolean acquireMutexLock(String identifier, long mutexTimeout, long pollTimeout, long pollInterval) {
        // Mark the start time
        Long start = new Date().time

        // Create a key
        String key = UUID.randomUUID().toString()

        // Loop until the timeout is reached
        while (pollTimeout == 0 || new Date().time < start + pollTimeout) {
            try {
                // Track success
                boolean locked = false

                // Run within a transaction
                DistributedMutex.withTransaction {
                    // Find the existing mutex
                    DistributedMutex mutex = DistributedMutex.findByIdentifier(identifier)

                    // If the mutex wasn't found, create one. Otherwise, force a refresh.
                    if (!mutex) {
                        mutex = new DistributedMutex(identifier: identifier)
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
                        log.warn("mutex identified by \"${identifier}\" is expired and has been reacquired by a new requester")
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
                    mutex.key = key
                    mutex.expires = expires
                    mutex.save(flush: true, failOnError: true)

                    // Mark it locked
                    locked = true
                }

                // Return now if the lock was acquired
                if (locked) {
                    log.debug("Successfully acquired mutex lock for identifier '${identifier}' with key '${key}'.")
                    return true
                }
            }
            catch (OptimisticLockingFailureException e) {
                log.warn("OptimisticLockingFailureException caught while attempting to acquire lock; will automatically retry if the acquire timeout has not expired")
            }
            catch (StaleObjectStateException e) {
                log.warn("StaleObjectStateException caught while attempting to acquire lock; will automatically retry if the acquire timeout has not expired")
            }
            catch (Exception e) {
                log.warn("unexpected exception '${e.class}' caught while attempting to acquire lock; will automatically retry if the acquire timeout has not expired", e)
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
     * Releases a mutex lock for a given identifier.
     *
     * @param identifier
     */
    void releaseMutexLock(String identifier) {
        try {
            DistributedMutex.withTransaction {
                // Find the mutex
                DistributedMutex mutex = DistributedMutex.findByIdentifier(identifier)

                // If one wasn't found, quit
                if (!mutex) {
                    log.warn("no mutex with identifier \"${identifier}\" was found")
                    return
                }

                // Mark it unlocked and save
                mutex.locked = false
                mutex.expires = null
                mutex.key = null
                mutex.save(flush: true, failOnError: true)
            }
        }
        catch (OptimisticLockingFailureException e) {
            log.warn("OptimisticLockingFailureException caught while attempting to release lock; will automatically retry")
        }
        catch (StaleObjectStateException e) {
            log.warn("StaleObjectStateException caught while attempting to release lock; will automatically retry")
        }
        catch (Exception e) {
            log.warn("unexpected exception '${e.class}' caught while attempting to acquire lock; will automatically retry", e)
        }
    }
}
