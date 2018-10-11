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
import org.apache.http.client.fluent.Request;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for single node {@link KVService} API
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
class SingleNodeTest extends TestBase {
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private static File data;
    private static KVDao dao;
    private static String endpoint;
    private static KVService storage;

    @BeforeAll
    static void beforeAll() throws IOException {
        final int port = randomPort();
        data = Files.createTempDirectory();
        dao = KVDaoFactory.create(data);
        endpoint = endpoint(port);
        storage = KVServiceFactory.create(port, dao, Collections.singleton(endpoint));
        storage.start();
    }

    @AfterAll
    static void afterAll() throws IOException {
        storage.stop();
        dao.close();
        Files.recursiveDelete(data);
    }

    @NotNull
    private String url(@NotNull final String id) {
        return endpoint + "/v0/entity?id=" + id;
    }

    @NotNull
    private String absentParameterUrl() {
        return endpoint + "/v0/entity";
    }

    private HttpResponse get(@NotNull final String key) throws IOException {
        return Request.Get(url(key)).execute().returnResponse();
    }

    private HttpResponse delete(@NotNull final String key) throws IOException {
        return Request.Delete(url(key)).execute().returnResponse();
    }

    private HttpResponse upsert(
            @NotNull final String key,
            @NotNull final byte[] data) throws IOException {
        return Request.Put(url(key)).bodyByteArray(data).execute().returnResponse();
    }

    @Test
    void emptyKey() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            assertEquals(400, get("").getStatusLine().getStatusCode());
            assertEquals(400, delete("").getStatusLine().getStatusCode());
            assertEquals(400, upsert("", new byte[]{0}).getStatusLine().getStatusCode());
        });
    }

    @Test
    void absentParameterRequest() {
        assertTimeoutPreemptively(TIMEOUT, () -> assertEquals(
                400,
                Request.Get(absentParameterUrl()).execute().returnResponse()
                        .getStatusLine().getStatusCode()));
    }

    @Test
    void badRequest() {
        assertTimeoutPreemptively(TIMEOUT, () -> assertEquals(
                404,
                Request.Get(url("/abracadabra")).execute().returnResponse()
                        .getStatusLine().getStatusCode()));
    }

    @Test
    void getAbsent() {
        assertTimeoutPreemptively(TIMEOUT, () -> assertEquals(
                404,
                get("absent").getStatusLine().getStatusCode()));
    }

    @Test
    void deleteAbsent() {
        assertTimeout(TIMEOUT, () -> assertEquals(202, delete("absent").getStatusLine().getStatusCode()));
    }

    @Test
    void insert() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(key, value).getStatusLine().getStatusCode());

            // Check
            final HttpResponse response = get(key);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertArrayEquals(value, payloadOf(response));
        });
    }

    @Test
    void insertEmpty() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = new byte[0];

            // Insert
            assertEquals(201, upsert(key, value).getStatusLine().getStatusCode());

            // Check
            final HttpResponse response = get(key);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertArrayEquals(value, payloadOf(response));
        });
    }

    @Test
    void lifecycle2keys() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key1 = randomId();
            final byte[] value1 = randomValue();
            final String key2 = randomId();
            final byte[] value2 = randomValue();

            // Insert 1
            assertEquals(201, upsert(key1, value1).getStatusLine().getStatusCode());

            // Check
            assertArrayEquals(value1, payloadOf(get(key1)));

            // Insert 2
            assertEquals(201, upsert(key2, value2).getStatusLine().getStatusCode());

            // Check
            assertArrayEquals(value1, payloadOf(get(key1)));
            assertArrayEquals(value2, payloadOf(get(key2)));

            // Delete 1
            assertEquals(202, delete(key1).getStatusLine().getStatusCode());

            // Check
            assertEquals(404, get(key1).getStatusLine().getStatusCode());
            assertArrayEquals(value2, payloadOf(get(key2)));

            // Delete 2
            assertEquals(202, delete(key2).getStatusLine().getStatusCode());

            // Check
            assertEquals(404, get(key2).getStatusLine().getStatusCode());
        });
    }

    @Test
    void upsert() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value1 = randomValue();
            final byte[] value2 = randomValue();

            // Insert value1
            assertEquals(201, upsert(key, value1).getStatusLine().getStatusCode());

            // Insert value2
            assertEquals(201, upsert(key, value2).getStatusLine().getStatusCode());

            // Check value 2
            final HttpResponse response = get(key);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertArrayEquals(value2, payloadOf(response));
        });
    }

    @Test
    void respectFileFolder() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert value
            assertEquals(201, upsert(key, value).getStatusLine().getStatusCode());

            // Check value
            final HttpResponse response = get(key);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertArrayEquals(value, payloadOf(response));

            // Remove data and recreate
            storage.stop();
            Files.recursiveDelete(data);
            final int port = randomPort();
            java.nio.file.Files.createDirectory(data.toPath());
            endpoint = endpoint(port);
            storage = KVServiceFactory.create(port, dao, Collections.singleton(endpoint));
            storage.start();

            // Check absent data
            assertEquals(404, get(key).getStatusLine().getStatusCode());
        });
    }

    @Test
    void upsertEmpty() {
        assertTimeout(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();
            final byte[] empty = new byte[0];

            // Insert value
            assertEquals(201, upsert(key, value).getStatusLine().getStatusCode());

            // Insert empty
            assertEquals(201, upsert(key, empty).getStatusLine().getStatusCode());

            // Check empty
            final HttpResponse response = get(key);
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertArrayEquals(empty, payloadOf(response));
        });
    }

    @Test
    void delete() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            final String key = randomId();
            final byte[] value = randomValue();

            // Insert
            assertEquals(201, upsert(key, value).getStatusLine().getStatusCode());

            // Delete
            assertEquals(202, delete(key).getStatusLine().getStatusCode());

            // Check
            assertEquals(404, get(key).getStatusLine().getStatusCode());
        });
    }
}
