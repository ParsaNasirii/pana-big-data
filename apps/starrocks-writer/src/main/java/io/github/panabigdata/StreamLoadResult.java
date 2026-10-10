package io.github.panabigdata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record StreamLoadResult(
    @JsonProperty("TxnId") Long txnId,
    @JsonProperty("Label") String label,
    @JsonProperty("Status") String status,
    @JsonProperty("Message") String message,
    @JsonProperty("ExistingJobStatus") String existingJobStatus,
    @JsonProperty("NumberTotalRows") Long numberTotalRows,
    @JsonProperty("NumberLoadedRows") Long numberLoadedRows,
    @JsonProperty("NumberFilteredRows") Long numberFilteredRows,
    @JsonProperty("NumberUnselectedRows") Long numberUnselectedRows,
    @JsonProperty("LoadBytes") Long loadBytes,
    @JsonProperty("LoadTimeMs") Long loadTimeMillis,
    @JsonProperty("ErrorURL") String errorUrl) {

  public boolean isSuccess() {
    return "Success".equals(status);
  }

  public boolean isPublishTimeout() {
    return "Publish Timeout".equals(status);
  }

  public boolean isLabelAlreadyExists() {
    return "Label Already Exists".equals(status);
  }

  public boolean isExistingJobFinished() {
    return isLabelAlreadyExists() && "FINISHED".equalsIgnoreCase(existingJobStatus);
  }

  public boolean isSafeToCommitKafkaOffsets() {
    return isAccepted() || isExistingJobFinished();
  }

  public boolean isAccepted() {
    return isSuccess() || isPublishTimeout();
  }
}
