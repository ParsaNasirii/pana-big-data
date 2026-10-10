package io.github.panabigdata;

import io.github.panabigdata.kafka.BatchWriter;
import io.github.panabigdata.kafka.WriteResult;
import io.github.panabigdata.metrics.StarRocksWriterMetrics;
import io.github.panabigdata.metrics.StarRocksWriterMetrics.Outcome;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
public class StarRocksWriterService implements BatchWriter {

  private final StarRocksStreamLoadClient streamLoadClient;
  private final StarRocksWriterMetrics metrics;
  private String pendingLoadLabel;
  private List<String> pendingColumns;
  private List<StarRocksNetFlowRow> pendingRows;

  public StarRocksWriterService(
      StarRocksStreamLoadClient streamLoadClient, StarRocksWriterMetrics metrics) {
    this.streamLoadClient =
        Objects.requireNonNull(streamLoadClient, "streamLoadClient must not be null");
    this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
  }

  @Override
  public WriteResult write(List<ConsumerRecord<byte[], GenericRecord>> records) throws IOException {
    Objects.requireNonNull(records, "records must not be null");

    if (records.isEmpty()) {
      return WriteResult.SUCCESS;
    }

    if (pendingLoadLabel == null) {
      GenericRecord first =
          Objects.requireNonNull(records.get(0).value(), "record value must not be null");
      List<String> columns =
          first.getSchema().getFields().stream().map(Schema.Field::name).toList();
      List<StarRocksNetFlowRow> rows =
          records.stream().map(record -> StarRocksNetFlowRow.from(record, columns)).toList();
      pendingLoadLabel = createLoadLabel();
      pendingColumns = columns;
      pendingRows = rows;
      metrics.setPendingRows(pendingRows.size());
    }

    StreamLoadResult result;
    long startedAt = System.nanoTime();
    try {
      result = streamLoadClient.load(pendingLoadLabel, pendingRows, pendingColumns);
    } catch (InterruptedException e) {
      metrics.recordLoad(Outcome.FAILED, System.nanoTime() - startedAt);
      Thread.currentThread().interrupt();
      log.warn("StarRocks Stream Load interrupted", e);
      return WriteResult.STOP;
    } catch (NonRetryableStreamLoadException e) {
      metrics.recordLoad(Outcome.FAILED, System.nanoTime() - startedAt);
      log.error("Non-retryable StarRocks Stream Load failure", e);
      return WriteResult.STOP;
    } catch (IOException e) {
      metrics.recordLoad(Outcome.RETRY, System.nanoTime() - startedAt);
      throw e;
    }

    if (!result.isSafeToCommitKafkaOffsets()) {
      metrics.recordLoad(Outcome.RETRY, System.nanoTime() - startedAt);
      log.warn(
          "StarRocks batch is not safe to commit. label={}, status={}, existingJobStatus={}, message={}, errorUrl={}",
          pendingLoadLabel,
          result.status(),
          result.existingJobStatus(),
          result.message(),
          result.errorUrl());
      return WriteResult.RETRY;
    }

    log.info(
        "StarRocks batch accepted. label={}, rows={}, loadBytes={}, loadTimeMs={}",
        pendingLoadLabel,
        pendingRows.size(),
        result.loadBytes(),
        result.loadTimeMillis());
    metrics.recordLoad(Outcome.SUCCESS, System.nanoTime() - startedAt);
    metrics.recordSuccessfulLoad(pendingRows.size());
    pendingLoadLabel = null;
    pendingColumns = null;
    pendingRows = null;
    return WriteResult.SUCCESS;
  }

  private static String createLoadLabel() {
    return "panabigdata_" + UUID.randomUUID().toString().replace("-", "");
  }
}
