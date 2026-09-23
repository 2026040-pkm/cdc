package dev.lidar.tagstream.ws;

import dev.lidar.tagstream.TagStreamProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final TagWebSocketHandler handler;
    private final TagStreamProperties.Ws props;

    public WebSocketConfig(TagWebSocketHandler handler, TagStreamProperties props) {
        this.handler = handler;
        this.props = props.ws();
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(handler, props.path()).setAllowedOriginPatterns(props.allowedOrigins());
    }

    /** 서버 → 클라이언트 한 프레임이 300KB 를 넘을 수 있다. 기본 8KB 버퍼로는 쪼개져 나간다. */
    @Bean
    public ServletServerContainerFactoryBean webSocketContainer() {
        var container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(props.bufferSizeLimitBytes());
        return container;
    }
}
