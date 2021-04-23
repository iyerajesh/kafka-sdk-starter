package com.xylia.platform.events;

import com.xylia.platform.events.config.YamlPropertyLoaderFactory;
import com.xylia.platform.events.configuration.SDKConfiguration;
import com.xylia.platform.events.consumer.ApplyEventConsumer;
import com.xylia.platform.events.health.KafkaConsumerMonitor;
import com.xylia.platform.events.health.KafkaHealthIndicator;
import com.xylia.platform.events.producer.ApplyEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import static org.springframework.util.Assert.isTrue;
import static org.springframework.util.Assert.notNull;


@Component
@Slf4j
@PropertySource(value = "classpath:sdk-config.yml", factory = YamlPropertyLoaderFactory.class)
public class ApplyEventPlatformSDK {

    private static final String APPLY_PLATFORM_EVENTS = "com.xylia.platform.events";
    private AnnotationConfigApplicationContext applicationContext;

    public void bootstrapSDK() {

        try {

            applicationContext = new AnnotationConfigApplicationContext();
            applicationContext.scan(APPLY_PLATFORM_EVENTS);
            applicationContext.refresh();

            log.info("SDK successfully bootstrapped with bean count: {}", applicationContext.getBeanDefinitionCount());

            isTrue(SDKConfiguration.getConsumerGroup().isPresent(),
                    "The SDK consumer group is set to the default!, Please set the consumer group, before consuming messages!");
        } catch (Exception bootStrapException) {
            log.error("SDK bootstrap failed with exception: ", bootStrapException);
            throw new IllegalStateException("SDK bootstrap failed with exception:", bootStrapException);
        }
    }

    public ApplyEventConsumer consumer() {
        notNull(this.applicationContext, "annotationApplicationContext cannot be null, is the SDK initialized?");
        return applicationContext.getBean(ApplyEventConsumer.class);
    }

    public ApplyEventProducer producer() {
        notNull(this.applicationContext, "annotationApplicationContext cannot be null, is the SDK initialized?");
        return applicationContext.getBean(ApplyEventProducer.class);
    }


    public KafkaHealthIndicator health() {
        notNull(this.applicationContext, "annotationApplicationContext cannot be null, is the SDK initialized?");
        return applicationContext.getBean(KafkaHealthIndicator.class);
    }

    public KafkaConsumerMonitor consumerMonitor() {
        notNull(this.applicationContext, "annotationApplicationContext cannot be null, is the SDK initialized?");
        return applicationContext.getBean(KafkaConsumerMonitor.class);
    }

}
