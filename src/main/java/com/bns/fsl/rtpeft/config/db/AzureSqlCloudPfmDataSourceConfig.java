package com.bns.fsl.rtpeft.config.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * Primary datasource: the Azure SQL Server database owned by this service,
 * holding eft_status and eft_config. This is the ONLY database this service
 * writes to. Repositories under com.bns.fsl.rtpeft.repository that map to
 * these tables are registered here as the primary JPA context.
 */
@Configuration
@EnableConfigurationProperties(PfmDataSourceConfigProperties.class)
@EnableJpaRepositories(
        basePackages = "com.bns.fsl.rtpeft.repository",
        entityManagerFactoryRef = "pfmEntityManagerFactory",
        transactionManagerRef = "pfmTransactionManager",
        excludeFilters = @org.springframework.context.annotation.ComponentScan.Filter(
                type = org.springframework.context.annotation.FilterType.ASSIGNABLE_TYPE,
                classes = com.bns.fsl.rtpeft.repository.FraudDecisionReadRepository.class)
)
public class AzureSqlCloudPfmDataSourceConfig {

    @Bean
    public DataSource pfmDataSource(PfmDataSourceConfigProperties props) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(props.getJdbcUrl());
        config.setUsername(props.getUsername());
        config.setPassword(props.getPassword());
        config.setMaximumPoolSize(props.getMaxPoolSize());
        config.setMinimumIdle(props.getMinIdle());
        config.setConnectionTimeout(props.getConnectionTimeoutMs());
        config.setPoolName("pfm-hikari-pool");
        return new HikariDataSource(config);
    }

    @Bean
    public EntityManagerFactory pfmEntityManagerFactory(EntityManagerFactoryBuilder builder, DataSource pfmDataSource) {
        Properties jpaProps = new Properties();
        jpaProps.put("hibernate.hbm2ddl.auto", "validate");
        jpaProps.put("hibernate.dialect", "org.hibernate.dialect.SQLServerDialect");
        return builder
                .dataSource(pfmDataSource)
                .packages("com.bns.fsl.rtpeft.entity")
                .persistenceUnit("pfm")
                .properties(jpaProps)
                .build()
                .getObject();
    }

    @Bean
    public PlatformTransactionManager pfmTransactionManager(EntityManagerFactory pfmEntityManagerFactory) {
        return new JpaTransactionManager(pfmEntityManagerFactory);
    }
}
