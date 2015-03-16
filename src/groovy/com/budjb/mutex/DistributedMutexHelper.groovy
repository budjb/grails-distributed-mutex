/*
 * Copyright 2015 Bud Byrd
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
import org.hibernate.StaleObjectStateException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.dao.OptimisticLockingFailureException

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
    Logger log = LoggerFactory.getLogger(DistributedMutexHelper)

    /**
     * Determines if a mutex is currently locked.
     */
    boolean isMutexLocked(String identifier) {
        DistributedMutex.withNewSession {
            return isMutexLocked(DistributedMutex.findByIdentifier(identifier))
        }
    }

    /**
     * Determines if a mutex is currently locked.
     */
    protected boolean isMutexLocked(DistributedMutex mutex) {
        // If there is no existing mutex, it cannot be locked
        if (!mutex) {
            return false
        }

        // Check if the mutex is unlocked
        if (!mutex.locked) {
            return false
        }

        // If the mutex is locked, and doesn't expire, keep the lock
        if (mutex.expires == null) {
            return true
        }

        // Finally, check whether the lock has expired
        if (mutex.expires.time >= System.currentTimeMillis()) {
            return true
        }

        return false
    }

    /**
     * Attempts to acquire a mutex lock for a given identifier.
     *
     * @param identifier Identifier of the mutex.
     * @param mutexTimeout Amount of time the mutex lock is valid until it can be reacquired.
     * @param pollTimeout Amount of time to wait for the mutex to become available.
     * @param pollInterval Amount of time to wait before checking on the mutex availability.
     * @return Mutex lock key required to release the mutex lock.
     */
    String acquireMutexLock(String identifier, long mutexTimeout = DEFAULT_MUTEX_TIMEOUT,
            long pollTimeout = DEFAULT_POLL_TIMEOUT, long pollInterval = DEFAULT_POLL_INTERVAL) throws LockNotAcquiredException {
        // Mark the start time
        Long start = System.currentTimeMillis()

        // Create a key
        String key = UUID.randomUUID()

        // Loop until the timeout is reached
        while (pollTimeout == 0 || start + pollTimeout > System.currentTimeMillis()) {
            // Track success
            boolean locked = false

            withTransaction true, {
                // Find the existing mutex
                DistributedMutex mutex = DistributedMutex.findByIdentifier(identifier)

                // If the mutex wasn't found, create one. Otherwise, force a refresh.
                if (!mutex) {
                    mutex = new DistributedMutex(identifier: identifier)
                }
                else {
                    mutex.refresh()

                    // Check if the mutex is locked
                    if (isMutexLocked(mutex)) {
                        return
                    }
                }

                // Log a warning if the mutex is expired
                if (mutex.expires && System.currentTimeMillis() > mutex.expires.time) {
                    log.warn("mutex identified by '{}' is expired and has been reacquired by a new requester", identifier)
                }

                // Determine the expiration time
                Date expires
                if (mutexTimeout > 0) {
                    expires = new Date(System.currentTimeMillis() + mutexTimeout)
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
                log.debug("Successfully acquired mutex lock for identifier '{}' with key '{}'.", identifier, key)
                return key
            }

            // If timeout is 0, just stop at one try
            if (pollTimeout == 0) {
                break
            }

            // Sleep a bit
            sleep pollInterval
        }

        throw new LockNotAcquiredException("mutex for identifier '$identifier' was unable to be acquired")
    }

    /**
     * Releases a mutex lock for a given identifier.
     *
     * @param identifier Identifier of the mutex.
     * @param key Key required to unlock the mutex.
     */
    void releaseMutexLock(String identifier, String key) {
        withTransaction {
            // Find the mutex
            DistributedMutex mutex = DistributedMutex.findByIdentifier(identifier)

            // If one wasn't found, quit
            if (!mutex) {
                log.warn("no mutex with identifier '{}' was found", identifier)
                return
            }

            // Check if the keys match
            if (mutex.key != key) {
                log.warn("the key on mutex with identifier '{}' does not match the key provided with the request to unlock the mutex", identifier)
                return
            }

            // Mark it unlocked and save
            mutex.locked = false
            mutex.expires = null
            mutex.key = null
            mutex.save(flush: true, failOnError: true)
        }
    }

    /**
     * Releases a mutex without requiring the key.
     *
     * WARNING: THIS IS DANGEROUS
     *
     * This method should only be used as a last resort to restore stability in an application.
     * The application logic should be robust enough so that processes that acquire a mutex have
     * the proper error handling to release it to avoid weird application states where the mutex
     * is no longer respected.
     *
     * @param identifier
     */
    void forciblyReleaseMutex(String identifier) {
        withTransaction {
            // Find the mutex
            DistributedMutex mutex = DistributedMutex.findByIdentifier(identifier)

            // If one wasn't found, quit
            if (!mutex) {
                log.warn("no mutex with identifier '{}' was found", identifier)
                return
            }

            // Mark it unlocked and save
            mutex.locked = false
            mutex.expires = null
            mutex.key = null
            mutex.save(flush: true, failOnError: true)

            log.warn("mutex with identifier '{}' was forcibly released", identifier)
        }
    }

    /**
     * Helper method to wrap a body of work with transaction error handling.
     *
     * @param ifNotExpired
     * @param c
     */
    protected void withTransaction(boolean ifNotExpired = false, Closure c) {
        String suffix = ifNotExpired ? 'if the acquire timeout has not expired' : ''
        try {
            DistributedMutex.withNewSession {
                DistributedMutex.withTransaction c
            }
        }
        catch (OptimisticLockingFailureException e) {
            log.warn("OptimisticLockingFailureException caught while attempting to release lock; will automatically retry {}", suffix)
        }
        catch (StaleObjectStateException e) {
            log.warn("StaleObjectStateException caught while attempting to release lock; will automatically retry {}", suffix)
        }
        catch (Exception e) {
            log.warn("unexpected exception '${e.class.name}' caught while attempting to acquire lock; will automatically retry " + suffix, e)
        }
    }
}
