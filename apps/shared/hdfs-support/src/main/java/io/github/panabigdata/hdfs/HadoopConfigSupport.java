package io.github.panabigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public final class HadoopConfigSupport {
  private HadoopConfigSupport() {}

  public static void addSiteResources(Configuration configuration, String directory) {
    Path path = new Path(directory);
    configuration.addResource(new Path(path, "core-site.xml"));
    configuration.addResource(new Path(path, "hdfs-site.xml"));
  }
}
