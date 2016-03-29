package com.tvelykyy.kafkabigquery.connector;

import com.tvelykyy.kafkabigquery.BigqueryStreamer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BigquerySinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(BigquerySinkTask.class);

    private String projectId;
    private String datasetId;
    private String tableId;

    private Converter converter;

    public BigquerySinkTask() {
        converter = new JsonConverter();
        Map<String, Object> configs = new HashMap<>();
        configs.put("schemas.enable", false);
        converter.configure(configs, false);
    }

    @Override
    public String version() {
        return "0.1";
    }

    @Override
    public void start(final Map<String, String> props) {
        LOG.info("Configuring Bigquery Sink task");
        projectId = props.get(BigquerySinkConnector.PROJECT_ID);
        datasetId = props.get(BigquerySinkConnector.DATASET_ID);
        tableId = props.get(BigquerySinkConnector.DATASET_ID);
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        LOG.info("Streaming {} records to bigquery", records.size());
        for (SinkRecord record : records) {
            byte[] json = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            LOG.debug("Streaming record to bigquery: {}", new String(json));

            try {
                BigqueryStreamer.streamJson(projectId, datasetId, tableId, json);
            } catch (IOException e) {
                LOG.error("Failed to stream record to bigquery: {}", e.getMessage());
            }
        }
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void stop() {
    }

}
