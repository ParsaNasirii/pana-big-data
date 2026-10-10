package io.github.panabigdata;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class BaseSparkTest {

  protected static SparkSession spark;

  @BeforeAll
  static void setupSpark() {

    spark =
        SparkSession.builder()
            .appName("SparkAnalysisTest")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void tearDownSpark() {
    if (spark != null) {
      spark.stop();
    }
  }
}
