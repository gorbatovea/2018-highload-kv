/*
 * Copyright 2018 (c) Vadim Tsesko <incubos@yandex.com>
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

package ru.mail.polis;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Starts 3-node storage cluster and waits for shutdown
 *
 * @author Vadim Tsesko <mail@incubos.org>
 */
public final class ContainerizedCluster {
    private static Integer[] ports;

    public static void main(String[] args) throws IOException {
        //Fetch ports
        ports = new Integer[args.length];
        for(int i = 0; i < args.length; i++) {
            ports[i] = Integer.parseInt(args[i]);
        }
        
        // Fill the topology
        final Set<String> topology = new HashSet<>(3);
        for (final Integer port : ports) {
            topology.add("http://localhost:" + port.toString());
        }
        
        // Start node
        final int port = ports[0];
        final File data = Files.createTempDirectory();
        final KVDao dao = KVDaoFactory.create(data);

        System.out.println("Starting node on port " + port + " and data at " + data);

        // Start the storage
        final KVService storage =
                KVServiceFactory.create(
                        port,
                        dao,
                        topology);
        storage.start();
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    storage.stop();
                    try {
                        dao.close();
                    } catch (IOException e) {
                        throw new RuntimeException("Can't close dao", e);
                    }
                }));
    }
}
