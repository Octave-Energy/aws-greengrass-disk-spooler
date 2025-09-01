/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */


package com.aws.greengrass.disk.spool;

import com.aws.greengrass.config.Configuration;
import com.aws.greengrass.dependency.Context;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class DiskSpoolTest {

    @TempDir
    Path currDir;

    @Test
    void GIVEN_topic_with_missing_splill_config_WHEN_building_DiskSpool_THEN_instanciation_fails() throws SQLException {

        Context context = null;
        int invalidDiskSpillThreshold = -52; // cannot be negative

        try {
            context = new Context();

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
        } finally {
            if (context != null) {
                try {
                    context.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }
}
