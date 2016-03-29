package com.tvelykyy.kafkabigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Example of Bigquery Streaming.
 */
public class BigqueryStreamer {
    private static final Bigquery bigquery = initBigquery();
    private static final Gson gson = new Gson();

    private BigqueryStreamer() {
    }

    private static Bigquery initBigquery() {
        try {
            return BigqueryServiceFactory.getService();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void streamJson(final String projectId, final String datasetId, final String tableId, final byte[] row) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(row);
        JsonReader jsonReader = new JsonReader(new InputStreamReader(in));

        Map<String, Object> rowData = gson.<Map<String, Object>>fromJson(jsonReader, (new HashMap<String, Object>()).getClass());
        streamRow(projectId, datasetId, tableId, new TableDataInsertAllRequest.Rows().setJson(rowData));
    }

    private static TableDataInsertAllResponse streamRow(final String projectId,
                                                       final String datasetId,
                                                       final String tableId,
                                                       final TableDataInsertAllRequest.Rows row) throws IOException {

        return bigquery
            .tabledata()
            .insertAll(projectId, datasetId, tableId,
                new TableDataInsertAllRequest().setRows(Collections.singletonList(row)))
            .execute();
    }
}
