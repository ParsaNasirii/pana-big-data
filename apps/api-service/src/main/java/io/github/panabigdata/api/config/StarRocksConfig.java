package io.github.panabigdata.api.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StarRocksConfig {
  @Bean
  @ConfigurationProperties("spring.datasource.starrocks")
  public DataSourceProperties starrocksDataSourceProperties() {
    return new DataSourceProperties();
  }

  @Bean(name = "starrocksDataSource")
  @ConfigurationProperties("spring.datasource.starrocks.configuration")
  public HikariDataSource starrocksDataSource(
      @Qualifier("starrocksDataSourceProperties") DataSourceProperties properties) {
    return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
  }
}
