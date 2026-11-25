/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.disk.spool;

import com.aws.greengrass.config.Configuration;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.mqttclient.spool.SpoolMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.QOS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Nucleus removes spooled messages from MQTT ack callback threads, up to maxInFlightPublishes (default 5) at once.
 */
class DiskSpoolConcurrencyTest {

    private static final int THREADS = 5;
    /**
     * Messages written to disk. Windows waits for every database write to reach the disk, so these are slow there.
     */
    private static final int MESSAGES = 1000;
    /**
     * Messages kept in memory. These cost no disk writes, and many of them give the threads more chances to race.
     */
    private static final int IN_MEMORY_MESSAGES = 20_000;

    @TempDir
    Path currDir;

    @Test
    void GIVEN_messages_on_disk_WHEN_removed_from_several_threads_THEN_no_rows_are_left() throws Exception {
        DiskSpoolDAO dao = new DiskSpoolDAO(currDir.resolve("spooler.db"));
        dao.initialize();
        try {
            for (long id = 0; id < MESSAGES; id++) {
                dao.insertSpoolMessage(message(id));
            }

            runConcurrently(id -> dao.removeSpoolMessageById(id), MESSAGES, 1);

            assertEquals(Collections.emptyList(), toList(dao.getAllSpoolMessageIds()));
        } finally {
            dao.close();
        }
    }

    @Test
    void GIVEN_messages_in_memory_and_on_disk_WHEN_removed_twice_from_several_threads_THEN_spool_is_empty()
            throws Exception {
        try (Context context = new Context()) {
            Configuration config = new Configuration(context);
            config.mergeMap(1000, Collections.singletonMap("configuration",
                    // the first IN_MEMORY_MESSAGES 5-byte messages stay in memory, the rest spills to disk
                    Collections.singletonMap("diskSpillThresholdBytes", IN_MEMORY_MESSAGES * 5)));
            DiskSpoolDAO dao = new DiskSpoolDAO(currDir.resolve("spooler.db"));
            dao.initialize();
            try {
                DiskSpool spool = new DiskSpool(config.getRoot(), dao);
                int total = IN_MEMORY_MESSAGES + MESSAGES / 2;
                for (long id = 0; id < total; id++) {
                    spool.add(id, message(id));
                }
                assertEquals(IN_MEMORY_MESSAGES * 5, spool.getInMemorySpoolSize());
                assertEquals(MESSAGES / 2, toList(dao.getAllSpoolMessageIds()).size());

                // Nucleus can remove the same id twice, e.g. after a non-retryable PUBACK
                runConcurrently(spool::removeMessageById, total, 2);

                assertEquals(0, spool.getInMemorySpoolSize());
                assertEquals(Collections.emptyList(), toList(spool.getAllMessageIds()));
            } finally {
                dao.close();
            }
        }
    }

    private interface Remover {
        void remove(long id) throws SQLException;
    }

    /**
     * Each thread removes every THREADS-th id below {@code count}, {@code times} times in a row, all threads starting
     * together.
     */
    private static void runConcurrently(Remover remover, int count, int times) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Void>> futures = new ArrayList<>();
        try {
            for (int t = 0; t < THREADS; t++) {
                int first = t;
                futures.add(executor.submit(() -> {
                    start.await();
                    for (long id = first; id < count; id += THREADS) {
                        for (int i = 0; i < times; i++) {
                            remover.remove(id);
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<Void> future : futures) {
                future.get(); // rethrows any failure
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
        }
    }

    private static List<Long> toList(Iterable<Long> ids) {
        List<Long> list = new ArrayList<>();
        ids.forEach(list::add);
        return list;
    }

    private static SpoolMessage message(long id) {
        return SpoolMessage.builder()
                .id(id)
                .request(Publish.builder()
                        .topic("spool")
                        .payload("Hello".getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.AT_LEAST_ONCE)
                        .build())
                .build();
    }
}
