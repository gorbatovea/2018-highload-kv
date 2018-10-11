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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for a two node {@link KVService} cluster
 *
 * @author Vadim Tsesko <mail@incubos.org>
 */
class TwoNodeTest extends ClusterTestBase {
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private int port0;
    private int port1;
    private File data0;
    private File data1;
    private KVDao dao0;
    private KVDao dao1;
    private KVService storage0;
    private KVService storage1;

    @BeforeEach
    void beforeEach() throws IOException {
        port0 = randomPort();
        port1 = randomPort();
        endpoints = new LinkedHashSet<>(Arrays.asList(endpoint(port0), endpoint(port1)));
        data0 = Files.createTempDirectory();
        dao0 = KVDaoFactory.create(data0);
        storage0 = KVServiceFactory.create(port0, dao0, endpoints);
        storage0.start();
        data1 = Files.createTempDirectory();
        dao1 = KVDaoFactory.create(data1);
        storage1 = KVServiceFactory.create(port1, dao1, endpoints);
        storage1.start();
    }

    @AfterEach
    void afterEach() throws IOException {
        storage0.stop();
        dao0.close();
        Files.recursiveDelete(data0);
        storage1.stop();
        dao1.close();
        Files.recursiveDelete(data1);
        endpoints = Collections.emptySet();
    }

    @Test
    void tooSmallRF() {
        assertTimeout(TIMEOUT, () -> {
            assertEquals(400, get(0, randomId(), 0, 2).getStatusLine().getStatusCode());
            assertEquals(400, upsert(0, randomId(), randomValue(), 0, 2).getStatusLine().getStatusCode());
            assertEquals(400, delete(0, randomId(), 0, 2).getStatusLine().getStatusCode());
        });
    }

    @Test
    void tooBigRF() {
        assertTimeout(TIMEOUT, () -> {
            assertEquals(400, get(0, randomId(), 3, 2).getStatusLine().getStatusCode());
            assertEquals(400, upsert(0, randomId(), randomValue(), 3, 2).getStatusLine().getStatusCode());
            assertEquals(400, delete(0, randomId(), 3, 2).getStatusLine().getStatusCode());
        });
    }

    @Test
    void unreachableRF() {
        assertTimeout(TIMEOUT, () -> {

            storage0.stop();
            assertEquals(504, get(1, randomId(), 2, 2).getStatusLine().getStatusCode());
            assertEquals(504, upsert(1, randomId(), randomValue(), 2, 2).getStatusLine().getStatusCode());
            assertEquals(504, delete(1, randomId(), 2, 2).getStatusLine().getStatusCode());
        });
    }

    @Test
    void overlapRead() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value, 1, 2).getStatusLine().getStatusCode());

            // Check
            final HttpResponse response = get(1, key, 2, 2);
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
            assertEquals(201, upsert(0, key, value, 2, 2).getStatusLine().getStatusCode());

            // Check
            final HttpResponse response = get(1, key, 1, 2);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertArrayEquals(value, payloadOf(response));
        });
    }

    @Test
    void overlapDelete() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value, 2, 2).getStatusLine().getStatusCode());

            // Check
            HttpResponse response = get(1, key, 1, 2);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertArrayEquals(value, payloadOf(response));

            // Delete
            assertEquals(202, delete(0, key, 2, 2).getStatusLine().getStatusCode());

            // Check
            response = get(1, key, 1, 2);
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
            assertEquals(201, upsert(0, key, value, 1, 2).getStatusLine().getStatusCode());

            // Start node 1
            storage1 = KVServiceFactory.create(port1, dao1, endpoints);
            storage1.start();

            // Check
            final HttpResponse response = get(1, key, 2, 2);
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
            assertEquals(201, upsert(0, key, value, 2, 2).getStatusLine().getStatusCode());

            // Stop node 0
            storage0.stop();

            // Delete
            assertEquals(202, delete(1, key, 1, 2).getStatusLine().getStatusCode());

            // Start node 0
            storage0 = KVServiceFactory.create(port0, dao0, endpoints);
            storage0.start();

            // Check
            final HttpResponse response = get(0, key, 2, 2);
            assertEquals(404, response.getStatusLine().getStatusCode());
        });
    }

    @Test
    void respectRF() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(0, key, value, 1, 1).getStatusLine().getStatusCode());

            int copies = 0;

            // Stop node 0
            storage0.stop();

            // Check
            if (get(1, key, 1, 1).getStatusLine().getStatusCode() == 200) {
                copies++;
            }

            // Start node 0
            storage0 = KVServiceFactory.create(port0, dao0, endpoints);
            storage0.start();

            // Stop node 1
            storage1.stop();

            // Check
            if (get(0, key, 1, 1).getStatusLine().getStatusCode() == 200) {
                copies++;
            }

            // Start node 1
            storage1 = KVServiceFactory.create(port1, dao1, endpoints);
            storage1.start();

            // Check
            assertEquals(1, copies);
        });
    }
}
