package io.github.panabigdata;

import io.github.panabigdata.backfill.Backfill;
import io.github.panabigdata.backfill.BackfillConfig;

public final class Main {
  public static void main(String[] args) throws Exception {
    new Backfill(new BackfillConfig()).run();
  }
}
