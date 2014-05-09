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
class DistributedMutexGrailsPlugin {
    /**
     * Plugin version.
     */
    def version = "0.2.1"

    /**
     * Grails version requirement.
     */
    def grailsVersion = "2.0 > *"

    /**
     * Plugin dependencies.
     */
    def dependsOn = [:]

    /**
     * Excluded resources.
     */
    def pluginExcludes = []

    /**
     * Plugin title.
     */
    def title = "Distributed Mutex Plugin"

    /**
     * Author.
     */
    def author = "Bud Byrd"

    /**
     * Author email.
     */
    def authorEmail = "bud.byrd@gmail.com"

    /**
     * Plugin description.
     */
    def description = '''\
This is a plugin that provides applications database-driven mutex functionality
to serialize parallel processes acting on the same resources.
'''

    /**
     * Documentation.
     */
    def documentation = "http://budjb.github.io/grails-distributed-mutex/doc/manual"

    /**
     * License.
     */
    def license = "APACHE"

    /**
     * Issue tracker.
     */
    def issueManagement = [system: 'GITHUB', url: 'https://github.com/budjb/grails-distributed-mutex/issues']

    /**
     * Online location of the plugin's browseable source code.
     */
    def scm = [url: 'https://github.com/budjb/grails-distributed-mutex']
}
