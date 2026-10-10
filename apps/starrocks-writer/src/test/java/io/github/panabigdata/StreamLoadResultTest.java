package io.github.panabigdata;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class StreamLoadResultTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void shouldParseSuccessfulStreamLoadResponse() throws Exception {
    String json =
        """
                {
                  "TxnId": 12345,
                  "Label": "panabigdata-test-label",
                  "Status": "Success",
                  "Message": "OK",
                  "NumberTotalRows": 10,
                  "NumberLoadedRows": 10,
                  "NumberFilteredRows": 0,
                  "NumberUnselectedRows": 0,
                  "LoadBytes": 2048,
                  "LoadTimeMs": 25,
                  "ErrorURL": ""
                }
                """;

    StreamLoadResult result = objectMapper.readValue(json, StreamLoadResult.class);

    assertThat(result.txnId()).isEqualTo(12345L);

    assertThat(result.label()).isEqualTo("panabigdata-test-label");

    assertThat(result.status()).isEqualTo("Success");

    assertThat(result.numberTotalRows()).isEqualTo(10L);

    assertThat(result.numberLoadedRows()).isEqualTo(10L);

    assertThat(result.numberFilteredRows()).isZero();

    assertThat(result.isSuccess()).isTrue();

    assertThat(result.isPublishTimeout()).isFalse();

    assertThat(result.isLabelAlreadyExists()).isFalse();

    assertThat(result.isAccepted()).isTrue();
  }

  @Test
  void shouldTreatPublishTimeoutAsAccepted() throws Exception {
    String json =
        """
                {
                  "Status": "Publish Timeout",
                  "Message": "publish timeout"
                }
                """;

    StreamLoadResult result = objectMapper.readValue(json, StreamLoadResult.class);

    assertThat(result.isSuccess()).isFalse();

    assertThat(result.isPublishTimeout()).isTrue();

    assertThat(result.isAccepted()).isTrue();
  }

  @Test
  void shouldNotTreatLabelAlreadyExistsAsAccepted() throws Exception {
    String json =
        """
                {
                  "Status": "Label Already Exists",
                  "Message": "Label already used"
                }
                """;

    StreamLoadResult result = objectMapper.readValue(json, StreamLoadResult.class);

    assertThat(result.isLabelAlreadyExists()).isTrue();

    assertThat(result.isAccepted()).isFalse();
  }

  @Test
  void shouldIgnoreUnknownFields() throws Exception {
    String json =
        """
                {
                  "Status": "Success",
                  "SomeFutureField": "future-value"
                }
                """;

    StreamLoadResult result = objectMapper.readValue(json, StreamLoadResult.class);

    assertThat(result.status()).isEqualTo("Success");

    assertThat(result.isAccepted()).isTrue();
  }

  @Test
  void shouldTreatFinishedExistingJobAsSafeToCommit() {
    StreamLoadResult result =
        new StreamLoadResult(
            null,
            "test-label",
            "Label Already Exists",
            "Label has already been used",
            "FINISHED",
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    assertThat(result.isLabelAlreadyExists()).isTrue();

    assertThat(result.isExistingJobFinished()).isTrue();

    assertThat(result.isAccepted()).isFalse();

    assertThat(result.isSafeToCommitKafkaOffsets()).isTrue();
  }

  @Test
  void shouldNotTreatRunningExistingJobAsSafeToCommit() {
    StreamLoadResult result =
        new StreamLoadResult(
            null,
            "test-label",
            "Label Already Exists",
            "Label has already been used",
            "RUNNING",
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    assertThat(result.isLabelAlreadyExists()).isTrue();

    assertThat(result.isExistingJobFinished()).isFalse();

    assertThat(result.isSafeToCommitKafkaOffsets()).isFalse();
  }
}
