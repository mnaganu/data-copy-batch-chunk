package com.example.mnaganu.dcbc.domain.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@lombok.Getter
@lombok.Setter
@Component
@ConfigurationProperties(prefix = "spring.datasource.copy-to")
public class CopyToDataSourceConfiguration {
    private String driverClassName;
    private String url;
    private String username;
    private String password;
    private int fetchsize;

    @Bean(name = "copyToDataSource")
    public DataSource createDataSource() {
        return DataSourceBuilder.create()
                .driverClassName(driverClassName)
                .url(url)
                .username(username)
                .password(password)
                .build();
    }
    
    @Bean(name = "copyToNamedParameterJdbcTemplate")
    public NamedParameterJdbcTemplate createNamedParameterJdbcTemplate(
            @Qualifier("copyToDataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean(name = "copyToFetchSize")
    public Integer createFetchSize() {
        return Integer.valueOf(fetchsize);
    }
    
}
