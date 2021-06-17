/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.streamnative.rocketmq.example.benchmark;

import org.apache.logging.log4j.util.Strings;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.remoting.RPCHook;

/**
 * Acl client.
 */
public class AclClient {

    private static final String ACL_ACCESS_KEY = "eyJrZXlJZCI6InB1bHNhci04xxxxxxxxxxUzI1NiJ9."
            + "eyJzdWIiOiJwdWxxxxxx3QtMTExMTExIn0.cIsxxGtnuXxxxxxES-0WccDZjPEGUFzT-th-f6I";

    static RPCHook getAclRPCHook() {
        return new AclClientRPCHook(new SessionCredentials(ACL_ACCESS_KEY, Strings.EMPTY));
    }
}
