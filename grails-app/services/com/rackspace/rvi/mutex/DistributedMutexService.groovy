package com.rackspace.rvi.mutex

import org.apache.log4j.Logger

class DistributedMutexService {
    /**
     * Disable transactional behavior.
     */
    static transactional = false

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
        return acquireMutexLock(key, 0)
    }

    /**
     * Attempts to acquire a mutex lock for a given key.
     *
     * @param key Key value that identifies the mutex.
     * @param timeout Amount of time to wait for the mutex to become available.
     * @return
     */
    boolean acquireMutexLock(String key, long timeout) {
        return acquireMutexLock(key, timeout, 500)
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
