package io.github.panabigdata;

import java.io.IOException;
import lombok.Getter;

@Getter
public class NonRetryableStreamLoadException extends IOException {

  private final int statusCode;

  public NonRetryableStreamLoadException(int statusCode, String responseBody) {
    super(
        "Stream Load HTTP request failed with non-retryable status "
            + statusCode
            + ": "
            + responseBody);
    this.statusCode = statusCode;
  }
}
