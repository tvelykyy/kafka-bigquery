package com.tvelykyy.kafkabigquery.connector;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigquerySinkConnector extends Connector {
    public static final String PROJECT_ID = "bigquery.project.id";
    public static final String DATASET_ID = "bigquery.dataset.id";
    public static final String TABLE_ID = "bigquery.table.id";
    public static final String TOPICS = "topics";

    private static final Logger LOG = LoggerFactory.getLogger(BigquerySinkConnector.class);

    private String projectId;
    private String datasetId;
    private String[] topics;

    @Override
    public String version() {
        //TODO implement versioning
        return "0.1";
    }

    @Override
    public void start(final Map<String, String> props) {
        LOG.info("Starting Bigquery Connector");
        projectId = props.get(PROJECT_ID);
        datasetId = props.get(DATASET_ID);
        topics = props.get(TOPICS).split(",");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return BigquerySinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        LOG.info("Preparing Bigquery Connector task configs with maxTasks {}", maxTasks);

        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskProps = new HashMap<>();
            taskProps.put(PROJECT_ID, projectId);
            taskProps.put(DATASET_ID, datasetId);
            taskProps.put(TABLE_ID, topics[i]);

            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
    }
}
