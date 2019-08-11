package com.indocms.gatewayservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class GatewayServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(GatewayServiceApplication.class, args);
	}

	@Bean
    public RouteLocator myRoutes(RouteLocatorBuilder builder) {
		return builder.routes()
			.route(p -> p				
				.path("/api/template/**")
				.filters(f -> f.stripPrefix(1))
				.uri("http://localhost:8081/template/**"))
			.route(p -> p
				.path("/api/postgresql/**")
				.filters(f -> f.stripPrefix(1))
				.uri("http://localhost:8082/postgresql/**"))
            .build();
    }

}
