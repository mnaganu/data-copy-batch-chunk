package com.example.mnaganu.dcbc.domain.config;

import org.springframework.boot.autoconfigure.batch.BatchDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@lombok.Getter
@lombok.Setter
@Component
@ConfigurationProperties(prefix = "spring.datasource.batch")
public class BatchDataSourceConfiguration {
    private String driverClassName;
    private String url;
    private String username;
    private String password;

    @BatchDataSource
    @Bean(name = "batchDataSource")
    public DataSource createDataSource() {
        return DataSourceBuilder.create()
                .driverClassName(driverClassName)
                .url(url)
                .username(username)
                .password(password)
                .build();
    }

}
