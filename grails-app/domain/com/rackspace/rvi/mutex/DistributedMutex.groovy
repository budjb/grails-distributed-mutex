package com.rackspace.rvi.mutex

class DistributedMutex {
    /**
     * Mutex key.
     */
    String keyValue

    /**
     * Whether the mutex is locked.
     */
    boolean locked = false

    /**
     * Timestamp when the mutex was last modified.
     */
    Date lastUpdated

    /**
     * Field constraints.
     */
    static constraints = {
        keyValue unique: true, nullable: false, blank: false
        lastUpdated nullable: true
    }
}
