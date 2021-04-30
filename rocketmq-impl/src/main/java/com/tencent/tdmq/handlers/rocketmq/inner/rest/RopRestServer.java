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

package com.tencent.tdmq.handlers.rocketmq.inner.rest;

import java.net.InetSocketAddress;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * Rop rest server.
 */
@Slf4j
public class RopRestServer {

//    public static void main(String[] args) {
//        start(9888);
//    }
//
//    public static void start(int port) {
//        log.info("Starting rocketmq nameserver HTTP server at port {}", port);
//        InetSocketAddress httpEndpoint = InetSocketAddress.createUnresolved("0.0.0.0", port);
//
//        Server server = new Server(httpEndpoint);
//        ServletContextHandler context = new ServletContextHandler();
//        context.setContextPath("/");
//        server.setHandler(context);
//        context.addServlet(new ServletHolder(new NameserverServlet()), "rocketmq/*");
//        try {
//            server.start();
//        } catch (Exception e) {
//            log.error(
//                    "Failed to start HTTP server at port {}. Use \"-Drocketmq_rest_server_port=1234\" "
//                            + "to change port number",
//                    port, e);
//            System.exit(0);
//        }
//    }
}
