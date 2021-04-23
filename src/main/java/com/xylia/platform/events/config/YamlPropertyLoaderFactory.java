package com.xylia.platform.events.config;

import com.xylia.platform.events.profile.SpringProfileDocumentMatcher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PropertySourceFactory;

import java.io.IOException;
import java.util.Properties;

import static com.xylia.platform.events.util.Util.getActiveRuntimeProfile;

@Slf4j
public class YamlPropertyLoaderFactory implements PropertySourceFactory {

    public static final String DEFAULT_PROFILE = "default";


    @Override
    public PropertySource<?> createPropertySource(String name, EncodedResource encodedResource) throws IOException {

        YamlPropertiesFactoryBean factoryBean = new YamlPropertiesFactoryBean();
        SpringProfileDocumentMatcher matcher = new SpringProfileDocumentMatcher();
        matcher.addActiveProfiles(mapActiveRuntimeProfile());

        factoryBean.setDocumentMatchers(matcher);
        factoryBean.setResources(encodedResource.getResource());

        Properties properties = factoryBean.getObject();
        return new PropertiesPropertySource(encodedResource.getResource().getFilename(), properties);
    }

    private static final String[] mapActiveRuntimeProfile() {

        String runtimeProfile = getActiveRuntimeProfile();
        String profileType = System.getProperty("PROFILE_TYPE");

        log.info("The Active profile in the SDK is: {}, and the profile type is: {}", runtimeProfile, profileType);

        if (runtimeProfile != null && profileType != null)
            return new String[]{DEFAULT_PROFILE, runtimeProfile + "-" + profileType};
        else if (runtimeProfile != null && profileType == null)
            return new String[]{DEFAULT_PROFILE, runtimeProfile};

        return new String[]{DEFAULT_PROFILE};
    }
}
