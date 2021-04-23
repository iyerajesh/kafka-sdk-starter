package com.xylia.platform.events.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.core.env.PropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
public class YamlPropertyLoaderFactoryTest {

    @InjectMocks
    private YamlPropertyLoaderFactory yamlPropertyLoaderFactory;

    @BeforeEach
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void createPropertySource_nullResource() throws Exception {
        Assertions.assertThrows(NullPointerException.class, () -> {
            PropertySource<?> propertySource = yamlPropertyLoaderFactory
                    .createPropertySource("somename", null);
        });
    }

}