/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.disk.spool;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttclient.spool.CloudMessageSpool;
import com.aws.greengrass.mqttclient.spool.InMemorySpool;
import com.aws.greengrass.mqttclient.spool.SpoolMessage;
import com.aws.greengrass.util.Coerce;

import java.io.IOException;
import java.sql.SQLException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.inject.Inject;

/** Disk Spooler with an optional spill threshold.
 * <p>
 * When spill threshold is 0 (default), all messages are persistedc which improves data delivery guarantees at the
 * expense of additional I/O and, in case of Flash storages, increased Flash wear.
 * </p>
 * <p>
 * When spill threshold is set to some positive number, messages are kept in memory until threshold is reached, at
 * which point the next messages are stored on the physical storage. By tuning this to the typical in-flight size
 * of the app, this helps to guarantee that physical storage is only used when application is offline, reducing I/O
 * and potentially Flash wear, at the expense of a small data loss.
 * </p>
 */
@ImplementsService(name = DiskSpool.PERSISTENCE_SERVICE_NAME, autostart = true)
public class DiskSpool extends PluginService implements CloudMessageSpool {

    public static final String PERSISTENCE_SERVICE_NAME = "aws.greengrass.DiskSpooler";
    private static final Logger logger = LogManager.getLogger(DiskSpool.class);
    private static final String KV_MESSAGE_ID = "messageId";
    private final DiskSpoolDAO dao;

    private final Long diskSpillThresholdBytes;
    private Long inMemorySpoolSize = 0L;
    private final InMemorySpool inMemorySpooler = new InMemorySpool();

    /**
     * constructor.
     * @param topics : plugin configuration
     * @param dao : actual db storage handler
     */
    @Inject
    public DiskSpool(Topics topics, DiskSpoolDAO dao) {
        super(topics);
        diskSpillThresholdBytes = Coerce.toLong(
                topics.findOrDefault(0, new String[]{"configuration", "diskSpillThresholdBytes"})
        );
        if (diskSpillThresholdBytes < 0) {
            throw new IllegalArgumentException("diskSpillThresholdBytes can't be negative: " + diskSpillThresholdBytes);
        }
        logger.info("diskSpillThresholdBytes: " + diskSpillThresholdBytes);
        this.dao = dao;
    }

    /**
     * This function takes an id and returns a SpoolMessage from the db with the same id.
     * @param id : id assigned to MQTT message
     * @return payload of the MQTT message stored with id
     */
    @Override
    public SpoolMessage getMessageById(long id) {
        try {
            SpoolMessage fromMemory = inMemorySpooler.getMessageById(id);
            if (fromMemory == null) {
                return dao.getSpoolMessageById(id);
            } else {
                return fromMemory;
            }
        } catch (SQLException e) {
            logger.atError()
                    .kv(KV_MESSAGE_ID, id)
                    .cause(e)
                    .log("Failed to retrieve message by messageId");
            return null;
        }
    }

    /**
     * This function takes an id and removes the row in the db with the corresponding id.
     * @param id : id assigned to MQTT message
     */
    @Override
    public void removeMessageById(long id) {
        try {
            SpoolMessage message = inMemorySpooler.getMessageById(id);
            if (message != null) {
                inMemorySpooler.removeMessageById(id);
                inMemorySpoolSize = Math.max(0, inMemorySpoolSize - message.getRequest().getPayload().length);
            }

            dao.removeSpoolMessageById(id);
            logger.atTrace().kv(KV_MESSAGE_ID, id).log("Removed message from Disk Spooler");
        } catch (SQLException e) {
            logger.atWarn()
                    .kv(KV_MESSAGE_ID, id)
                    .cause(e)
                    .log("Failed to delete message by messageId");
        }
    }

    /**
     * This function takes in an id and SpoolMessage and inserts them as a row into the spooler db.
     * @param id : id assigned to MQTT message
     * @param message :
     */
    @Override
    public void add(long id, SpoolMessage message) throws IOException {
        if (inMemorySpoolSize < diskSpillThresholdBytes) {
            // this is meant to happen most of the time: while number of in-flight message is low, they're only
            // kept in memory
            inMemorySpooler.add(id, message);
            inMemorySpoolSize += message.getRequest().getPayload().length;
        } else {
            // when memory is full: spill over to disk
            try {
                dao.insertSpoolMessage(message);
                logger.atTrace().kv(KV_MESSAGE_ID, id).log("Added message to Disk Spooler");
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public Iterable<Long> getAllMessageIds() throws IOException {
        try {
            Iterable<Long> fromDao = dao.getAllSpoolMessageIds();
            return () -> Stream.concat(
                    inMemorySpooler.getAllMessageIds().stream(),
                    StreamSupport.stream(fromDao.spliterator(), false)
            ).iterator();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void initializeSpooler() throws IOException {
        try {
            inMemorySpooler.initializeSpooler();
            dao.initialize();
            logger.atInfo().log("Finished setting up Database");
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void shutdown() throws InterruptedException {
        super.shutdown();
        dao.close();
    }
}
