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

import org.apache.http.HttpResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for a three node {@link KVService} cluster
 *
 * @author Vadim Tsesko <mail@incubos.org>
 */
class ThreeNodeTest extends ClusterTestBase {
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private int port0;
    private int port1;
    private int port2;
    private File data0;
    private File data1;
    private File data2;
    private KVDao dao0;
    private KVDao dao1;
    private KVDao dao2;
    private KVService storage0;
    private KVService storage1;
    private KVService storage2;

    @BeforeEach
    void beforeEach() throws IOException {
        port0 = randomPort();
        port1 = randomPort();
        port2 = randomPort();
        endpoints = new LinkedHashSet<>(Arrays.asList(endpoint(port0), endpoint(port1), endpoint(port2)));
        data0 = Files.createTempDirectory();
        data1 = Files.createTempDirectory();
        data2 = Files.createTempDirectory();
        dao0 = KVDaoFactory.create(data0);
        dao1 = KVDaoFactory.create(data1);
        dao2 = KVDaoFactory.create(data2);
        storage0 = KVServiceFactory.create(port0, dao0, endpoints);
        storage0.start();
        storage1 = KVServiceFactory.create(port1, dao1, endpoints);
        storage1.start();
        storage2 = KVServiceFactory.create(port2, dao2, endpoints);
        storage2.start();
    }

    @AfterEach
    void afterEach() throws IOException {
        storage0.stop();
        dao0.close();
        Files.recursiveDelete(data0);
        storage1.stop();
        dao1.close();
        Files.recursiveDelete(data1);
        storage2.stop();
        dao2.close();
        Files.recursiveDelete(data2);
        endpoints = Collections.emptySet();
    }

    @Test
    void tooSmallRF() {
        assertTimeout(TIMEOUT, () -> {
            assertEquals(400, get(0, randomId(), 0, 3).getStatusLine().getStatusCode());
            assertEquals(400, upsert(0, randomId(), randomValue(), 0, 3).getStatusLine().getStatusCode());
            assertEquals(400, delete(0, randomId(), 0, 3).getStatusLine().getStatusCode());
        });
    }

    @Test
    void tooBigRF() {
        assertTimeout(TIMEOUT, () -> {
            assertEquals(400, get(0, randomId(), 4, 3).getStatusLine().getStatusCode());
            assertEquals(400, upsert(0, randomId(), randomValue(), 4, 3).getStatusLine().getStatusCode());
            assertEquals(400, delete(0, randomId(), 4, 3).getStatusLine().getStatusCode());
        });
    }

    @Test
    void unreachableRF() {
        assertTimeout(TIMEOUT, () -> {
            storage0.stop();
            assertEquals(504, get(1, randomId(), 3, 3).getStatusLine().getStatusCode());
            assertEquals(504, upsert(1, randomId(), randomValue(), 3, 3).getStatusLine().getStatusCode());
            assertEquals(504, delete(1, randomId(), 3, 3).getStatusLine().getStatusCode());
        });
    }

    @Test
    void overlapRead() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value, 2, 3).getStatusLine().getStatusCode());

            // Check
            final HttpResponse response = get(1, key, 2, 3);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertArrayEquals(value, payloadOf(response));
        });
    }

    @Test
    void overlapWrite() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value, 2, 3).getStatusLine().getStatusCode());

            // Check
            final HttpResponse response = get(1, key, 2, 3);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertArrayEquals(value, payloadOf(response));
        });
    }

    @Test
    void overwrite() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value1 = randomValue();
            final byte[] value2 = randomValue();

            // Insert 1
            assertEquals(201, upsert(0, key, value1, 2, 3).getStatusLine().getStatusCode());

            // Check 1
            HttpResponse response = get(1, key, 2, 3);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertArrayEquals(value1, payloadOf(response));

            // Help implementors with second precision for conflict resolution
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));

            // Insert 2
            assertEquals(201, upsert(2, key, value2, 2, 3).getStatusLine().getStatusCode());

            // Check 2
            response = get(1, key, 2, 3);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertArrayEquals(value2, payloadOf(response));
        });
    }

    @Test
    void overlapDelete() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value, 2, 3).getStatusLine().getStatusCode());

            // Check
            HttpResponse response = get(1, key, 2, 3);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertArrayEquals(value, payloadOf(response));

            // Delete
            assertEquals(202, delete(0, key, 2, 3).getStatusLine().getStatusCode());

            // Check
            response = get(1, key, 2, 3);
            assertEquals(404, response.getStatusLine().getStatusCode());
        });
    }

    @Test
    void missedWrite() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Stop node 1
            storage1.stop();

            // Insert
            assertEquals(201, upsert(0, key, value, 2, 3).getStatusLine().getStatusCode());

            // Start node 1
            storage1 = KVServiceFactory.create(port1, dao1, endpoints);
            storage1.start();

            // Check
            final HttpResponse response = get(1, key, 2, 3);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertArrayEquals(value, payloadOf(response));
        });
    }

    @Test
    void missedDelete() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value, 2, 3).getStatusLine().getStatusCode());

            // Stop node 0
            storage0.stop();

            // Delete
            assertEquals(202, delete(1, key, 2, 3).getStatusLine().getStatusCode());

            // Start node 0
            storage0 = KVServiceFactory.create(port0, dao0, endpoints);
            storage0.start();

            // Check
            final HttpResponse response = get(0, key, 2, 3);
            assertEquals(404, response.getStatusLine().getStatusCode());
        });
    }

    @Test
    void tolerateFailure() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert into node 2
            assertEquals(201, upsert(2, key, value, 2, 3).getStatusLine().getStatusCode());

            // Stop node 2
            storage2.stop();

            // Check
            HttpResponse response = get(1, key, 2, 3);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertArrayEquals(value, payloadOf(response));

            // Delete
            assertEquals(202, delete(0, key, 2, 3).getStatusLine().getStatusCode());

            // Check
            response = get(1, key, 2, 3);
            assertEquals(404, response.getStatusLine().getStatusCode());
        });
    }

    @Test
    void respectRF1() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value, 1, 1).getStatusLine().getStatusCode());

            int copies = 0;

            // Stop all nodes
            storage0.stop();
            storage1.stop();
            storage2.stop();

            // Start node 0
            storage0 = KVServiceFactory.create(port0, dao0, endpoints);
            storage0.start();

            // Check node 0
            if (get(0, key, 1, 1).getStatusLine().getStatusCode() == 200) {
                copies++;
            }

            // Stop node 0
            storage0.stop();

            // Start node 1
            storage1 = KVServiceFactory.create(port1, dao1, endpoints);
            storage1.start();

            // Check node 1
            if (get(1, key, 1, 1).getStatusLine().getStatusCode() == 200) {
                copies++;
            }

            // Stop node 1
            storage1.stop();

            // Start node 2
            storage2 = KVServiceFactory.create(port2, dao2, endpoints);
            storage2.start();

            // Check node 2
            if (get(2, key, 1, 1).getStatusLine().getStatusCode() == 200) {
                copies++;
            }

            // Start node 0 & 1
            storage0 = KVServiceFactory.create(port0, dao0, endpoints);
            storage0.start();
            storage1 = KVServiceFactory.create(port1, dao1, endpoints);
            storage1.start();

            // Check
            assertEquals(1, copies);
        });
    }

    @Test
    void respectRF2() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value, 2, 2).getStatusLine().getStatusCode());

            int copies = 0;

            // Stop all nodes
            storage0.stop();
            storage1.stop();
            storage2.stop();

            // Start node 0
            storage0 = KVServiceFactory.create(port0, dao0, endpoints);
            storage0.start();

            // Check node 0
            if (get(0, key, 1, 2).getStatusLine().getStatusCode() == 200) {
                copies++;
            }

            // Stop node 0
            storage0.stop();

            // Start node 1
            storage1 = KVServiceFactory.create(port1, dao1, endpoints);
            storage1.start();

            // Check node 1
            if (get(1, key, 1, 2).getStatusLine().getStatusCode() == 200) {
                copies++;
            }

            // Stop node 1
            storage1.stop();

            // Start node 2
            storage2 = KVServiceFactory.create(port2, dao2, endpoints);
            storage2.start();

            // Check node 2
            if (get(2, key, 1, 2).getStatusLine().getStatusCode() == 200) {
                copies++;
            }

            // Start node 0 & 1
            storage0 = KVServiceFactory.create(port0, dao0, endpoints);
            storage0.start();
            storage1 = KVServiceFactory.create(port1, dao1, endpoints);
            storage1.start();

            // Check
            assertEquals(2, copies);
        });
    }
}
