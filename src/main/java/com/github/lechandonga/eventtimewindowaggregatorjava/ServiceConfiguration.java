package com.github.lechandonga.eventtimewindowaggregatorjava;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.eventtimewindowaggregatorjava.proc.ProcessingService;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(ServiceProperties.class)
public class ServiceConfiguration {

    @Bean(destroyMethod = "stop")
    public ProcessingService processingService(ServiceProperties props, ObjectMapper mapper) {
        ObjectMapper pipelineMapper = new ObjectMapper();
        return new ProcessingService(props, pipelineMapper);
    }
}
