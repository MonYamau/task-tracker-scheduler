package ru.monyamau.task_tracker_scheduler.config;

import feign.RetryableException;
import feign.Retryer;
import feign.codec.ErrorDecoder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InternalFeignConfig {
    private static final int REQUEST_LIMITING_ERROR = 429;
    private static final int INTERNAL_SERVER_ERROR = 500;
    private static final int BAD_GATEWAY_ERROR = 502;
    private static final int SERVICE_UNAVAILABLE_ERROR = 503;
    private static final int GATEWAY_TIMEOUT_ERROR = 504;
    
    private static final long PERIOD_MS = 1000L;
    private static final long MAX_PERIOD_MS = 10000L;
    private static final int NUM_OF_ATTEMPTS = 3;

    @Bean
    public ErrorDecoder errorDecoder() {
        ErrorDecoder errorDecoder = new ErrorDecoder.Default();
        return (methodKey, response) -> {
            Exception decodedException = errorDecoder.decode(methodKey, response);
            int status = response.status();
            if (status != REQUEST_LIMITING_ERROR && status != INTERNAL_SERVER_ERROR && status != BAD_GATEWAY_ERROR
                    && status != SERVICE_UNAVAILABLE_ERROR && status != GATEWAY_TIMEOUT_ERROR) {
                return decodedException;
            }
            if (decodedException instanceof RetryableException) {
                return decodedException;
            }
            return new RetryableException(status, decodedException.getMessage(), response.request().httpMethod(),
                    (Long) null, response.request()
            );
        };
    }

    @Bean
    public Retryer retryer() {
        return new Retryer.Default(PERIOD_MS, MAX_PERIOD_MS, NUM_OF_ATTEMPTS);
    }
}