/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */


package com.aws.greengrass.disk.spool;

import com.aws.greengrass.config.Configuration;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.mqttclient.spool.SpoolMessage;
import com.aws.greengrass.mqttclient.spool.SpoolMessageMetadata;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.QOS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DiskSpoolTest {

    @TempDir
    Path currDir;

    @Test
    void GIVEN_topic_with_missing_splill_config_WHEN_building_DiskSpool_THEN_instanciation_fails() throws SQLException, IOException {
        try (Context context = new Context()) {
            int invalidDiskSpillThreshold = -52; // cannot be negative
            Configuration config = new Configuration(context);
            config.mergeMap(
                    1000,
                    Collections.singletonMap("configuration",
                            Collections.singletonMap("diskSpillThresholdBytes", invalidDiskSpillThreshold)
                    )
            );
            DiskSpoolDAOFake daoFake = new DiskSpoolDAOFake(currDir.resolve("spooler.db"));

            assertThrows(IllegalArgumentException.class, () -> {
                new DiskSpool(config.getRoot(), daoFake);
            });
        }
    }


    @Test
    void GIVEN_traffic_is_low_WHEN_storing_messages_in_diskspool_THEN_nothing_should_spill_to_disk() throws SQLException, IOException {

        // This validates backward compatibility with previous behavior without spill threshold:
        // setting the threshold to 0 should mean all messages are persisted

        try (Context context = new Context()) {
            Configuration config = new Configuration(context);
            config.mergeMap(
                    1000,
                    Collections.singletonMap("configuration",
                            Collections.singletonMap("diskSpillThresholdBytes", 0)
                    )
            );
            DiskSpoolDAOFake daoFake = new DiskSpoolDAOFake(currDir.resolve("spooler.db"));
            daoFake.initialize();
            // close the database at the end: Windows cannot delete the temporary directory of an open database
            try {
                DiskSpool spool = new DiskSpool(config.getRoot(), daoFake);

                for (long id = 1L; id <= 10000L; id++) {
                    SpoolMessage message = SpoolMessage.builder()
                            .id(id)
                            .request(
                                    Publish.builder()
                                            .topic("spool")
                                            .payload("Hello".getBytes(StandardCharsets.UTF_8))
                                            .qos(QOS.AT_LEAST_ONCE)
                                            .messageExpiryIntervalSeconds(2L)
                                            .payloadFormat(Publish.PayloadFormatIndicator.BYTES)
                                            .contentType("Test")
                                            .build())
                            .build();
                    spool.add(id, message);

                    // assert we can read it back from spool:
                    SpoolMessage fromSpool = spool.getMessageById(id);
                    assert fromSpool != null;
                    assert fromSpool.getId() == id;
                    assert new String(fromSpool.getRequest().getPayload(), StandardCharsets.UTF_8).equals("Hello");

                    // assert present in disk dao:
                    SpoolMessage fromDisk = daoFake.getSpoolMessageById(id);
                    assert fromDisk != null;
                    assert fromDisk.getId() == id;
                    assert new String(fromDisk.getRequest().getPayload(), StandardCharsets.UTF_8).equals("Hello");
                }
            } finally {
                daoFake.close();
            }
        }
    }


    @Test
    void GIVEN_spill_threshold_is_set_WHEN_storing_messages_in_diskspool_THEN_message_should_spill_over_to_dick() throws SQLException, IOException {

        // This validates the new behavior with spill threshold

        try (Context context = new Context()) {

            Configuration config = new Configuration(context);
            config.mergeMap(
                    1000,
                    Collections.singletonMap("configuration",
                            // 50 bytes : 10 first instances should be in memory, then everything should spill to disk
                            Collections.singletonMap("diskSpillThresholdBytes", 50)
                    )
            );
            DiskSpoolDAOFake daoFake = new DiskSpoolDAOFake(currDir.resolve("spooler.db"));
            daoFake.initialize();
            // close the database at the end: Windows cannot delete the temporary directory of an open database
            try {
                DiskSpool spool = new DiskSpool(config.getRoot(), daoFake);

                for (long id = 1L; id <= 10000L; id++) {
                    SpoolMessage message = SpoolMessage.builder()
                            .id(id)
                            .request(
                                    Publish.builder()
                                            .topic("spool")
                                            .payload("Hello".getBytes(StandardCharsets.UTF_8))
                                            .qos(QOS.AT_LEAST_ONCE)
                                            .messageExpiryIntervalSeconds(2L)
                                            .payloadFormat(Publish.PayloadFormatIndicator.BYTES)
                                            .contentType("Test")
                                            .build())
                            .build();
                    spool.add(id, message);

                    // everything should always be readable from spool:
                    SpoolMessage fromSpool = spool.getMessageById(id);
                    assert fromSpool != null;
                    assert fromSpool.getId() == id;
                    assert new String(fromSpool.getRequest().getPayload(), StandardCharsets.UTF_8).equals("Hello");

                    SpoolMessage fromDisk = daoFake.getSpoolMessageById(id);
                    if (id <= 10L) {
                        // first ten messages should be only in memory:
                        assert fromDisk == null;
                    } else {
                        // next ones should be spilled to disk:
                        assert fromDisk != null;
                        assert fromDisk.getId() == id;
                        assert new String(fromDisk.getRequest().getPayload(), StandardCharsets.UTF_8).equals("Hello");
                    }
                }
            } finally {
                daoFake.close();
            }
        }

    }

    @Test
    void GIVEN_messages_in_memory_and_on_disk_WHEN_listing_ids_and_metadata_THEN_both_are_merged_in_id_order()
            throws SQLException, IOException {
        try (Context context = new Context()) {
            Configuration config = new Configuration(context);
            config.mergeMap(1000, Collections.singletonMap("configuration",
                    // two 5-byte messages fit in memory, the next one spills to disk
                    Collections.singletonMap("diskSpillThresholdBytes", 10)));
            DiskSpoolDAOFake daoFake = new DiskSpoolDAOFake(currDir.resolve("spooler.db"));
            daoFake.initialize();
            DiskSpool spool = new DiskSpool(config.getRoot(), daoFake);

            spool.add(1L, message(1L, QOS.AT_LEAST_ONCE)); // memory
            spool.add(2L, message(2L, QOS.AT_MOST_ONCE));  // memory
            spool.add(3L, message(3L, QOS.AT_LEAST_ONCE)); // disk
            spool.removeMessageById(1L);                     // frees memory
            spool.add(4L, message(4L, QOS.AT_LEAST_ONCE)); // memory again

            assertNull(daoFake.getSpoolMessageById(2L));
            assertNotNull(daoFake.getSpoolMessageById(3L));
            assertNull(daoFake.getSpoolMessageById(4L));

            List<Long> ids = new ArrayList<>();
            spool.getAllMessageIds().forEach(ids::add);
            assertEquals(Arrays.asList(2L, 3L, 4L), ids);

            assertEquals(Arrays.asList(
                    new SpoolMessageMetadata(2L, 5L, QOS.AT_MOST_ONCE),
                    new SpoolMessageMetadata(3L, 5L, QOS.AT_LEAST_ONCE),
                    new SpoolMessageMetadata(4L, 5L, QOS.AT_LEAST_ONCE)
            ), spool.getAllMessageMetadata());
        }
    }

    private static SpoolMessage message(long id, QOS qos) {
        return SpoolMessage.builder()
                .id(id)
                .request(Publish.builder()
                        .topic("spool")
                        .payload("Hello".getBytes(StandardCharsets.UTF_8))
                        .qos(qos)
                        .build())
                .build();
    }

}
