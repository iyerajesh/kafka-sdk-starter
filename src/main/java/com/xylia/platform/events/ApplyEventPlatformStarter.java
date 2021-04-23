package com.xylia.platform.events;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableMBeanExport;

@SpringBootApplication
@EnableMBeanExport(defaultDomain = "apply-platform-sdk-starter")
@Slf4j
public class ApplyEventPlatformStarter {

    public static void main(String[] args) {
        final ApplyEventPlatformSDK applyEventPlatformSDK = new ApplyEventPlatformSDK();
        applyEventPlatformSDK.bootstrapSDK();

        log.debug("Kafka health:" + applyEventPlatformSDK.health().health().getStatus());
    }
}
