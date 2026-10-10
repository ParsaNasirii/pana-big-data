package io.github.panabigdata.maintenance;

import io.github.panabigdata.ConfigSupport;
import io.github.panabigdata.hdfs.HadoopConfigSupport;
import io.github.panabigdata.maintenance.compaction.ParquetCompactor;
import io.github.panabigdata.maintenance.config.MaintenanceConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

public final class Main {
  private Main() {}

  public static void main(String[] args) {
    MaintenanceConfig config = new MaintenanceConfig();
    try (SparkSession spark = createSparkSession(System.getenv())) {
      Configuration hadoop = spark.sparkContext().hadoopConfiguration();
      HadoopConfigSupport.addSiteResources(hadoop, config.getHadoopConfigDirectory());

      ParquetCompactor compactor =
          new ParquetCompactor(spark, config, Clock.system(config.getTimeZone()));
      compactor.compactPreviousDays();
    } catch (IOException failure) {
      throw new UncheckedIOException("Parquet maintenance failed", failure);
    }
  }

  private static SparkSession createSparkSession(Map<String, String> environment) {
    return SparkSession.builder()
        .appName("Pana Big Data Parquet Maintenance")
        .master(ConfigSupport.requiredEnvironment(environment, "SPARK_MASTER"))
        .config(
            "spark.driver.host",
            ConfigSupport.requiredEnvironment(environment, "SPARK_DRIVER_HOST"))
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config(
            "spark.driver.port",
            ConfigSupport.requiredEnvironment(environment, "SPARK_DRIVER_PORT"))
        .config(
            "spark.driver.blockManager.port",
            ConfigSupport.requiredEnvironment(environment, "SPARK_DRIVER_BLOCK_MANAGER_PORT"))
        .config(
            "spark.executor.memory",
            ConfigSupport.requiredEnvironment(environment, "SPARK_EXECUTOR_MEMORY"))
        .config(
            "spark.executor.cores",
            ConfigSupport.requiredEnvironment(environment, "SPARK_EXECUTOR_CORES"))
        .config(
            "spark.cores.max",
            ConfigSupport.requiredEnvironment(environment, "SPARK_TOTAL_EXECUTOR_CORES"))
        .config(
            "spark.executorEnv.HADOOP_USER_NAME",
            ConfigSupport.requiredEnvironment(environment, "HADOOP_USER_NAME"))
        .getOrCreate();
  }
}
