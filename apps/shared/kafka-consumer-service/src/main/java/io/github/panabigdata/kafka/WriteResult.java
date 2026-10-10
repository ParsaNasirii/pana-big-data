package io.github.panabigdata.kafka;

public enum WriteResult {
  SUCCESS_NO_COMMIT,
  SUCCESS,
  RETRY,
  STOP
}
