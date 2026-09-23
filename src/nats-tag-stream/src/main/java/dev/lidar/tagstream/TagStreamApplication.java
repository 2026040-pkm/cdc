package dev.lidar.tagstream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class TagStreamApplication {

    public static void main(String[] args) {
        SpringApplication.run(TagStreamApplication.class, args);
    }
}
