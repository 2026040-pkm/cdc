package dev.lidar.tagstream;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("tag-stream")
public record TagStreamProperties(Nats nats, Ws ws) {

    public record Nats(String url, String subject) {
    }

    public record Ws(String path, int sendTimeLimitMs, int bufferSizeLimitBytes, String allowedOrigins) {
    }
}
