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

import org.apache.http.client.fluent.Request;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;

/**
 * Basic init/deinit test for {@link KVService} implementation
 *
 * @author Vadim Tsesko <incubos@yandex.com>
 */
class StartStopTest extends TestBase {
    private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(1);
    private static final Duration TIMEOUT = Duration.ofMillis(TIMEOUT_MS * 2);

    private int port;
    private File data;
    private KVDao dao;
    private KVService kvService;

    @BeforeEach
    void beforeEach() throws IOException {
        data = Files.createTempDirectory();
        dao = KVDaoFactory.create(data);
        port = randomPort();
        kvService = KVServiceFactory.create(port, dao, Collections.singleton(endpoint(port)));
    }

    @AfterEach
    void afterEach() throws IOException {
        dao.close();
        kvService.stop();
        Files.recursiveDelete(data);
    }

    private static int status(int port) throws IOException {
        return Request.Get("http://localhost:" + port + "/v0/status")
                .connectTimeout((int) TIMEOUT_MS)
                .socketTimeout((int) TIMEOUT_MS)
                .execute()
                .returnResponse()
                .getStatusLine()
                .getStatusCode();
    }

    @Test
    void create() {
        assertTimeout(TIMEOUT, () -> {
            assertThrows(IOException.class, () -> status(port));
        });
    }

    @Test
    void start() {
        assertTimeout(TIMEOUT, () -> {
            kvService.start();
            assertEquals(200, status(port));
        });
    }

    @Test
    void stop() {
        assertTimeout(TIMEOUT, () -> {
            kvService.start();
            assertEquals(200, status(port));
            kvService.stop();
            // Should not respond after stop
            assertThrows(IOException.class, () -> status(port));
        });
    }
}
