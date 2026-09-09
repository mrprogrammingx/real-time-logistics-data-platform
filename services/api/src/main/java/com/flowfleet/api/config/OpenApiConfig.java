package com.flowfleet.api.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class OpenApiConfig {

    @Bean
    OpenAPI flowFleetOpenApi() {
        return new OpenAPI().info(new Info()
                .title("FlowFleet Operational API")
                .version("0.1.0")
                .description("OLTP endpoints for orders and drivers. Swagger UI at /swagger-ui.html."));
    }
}
