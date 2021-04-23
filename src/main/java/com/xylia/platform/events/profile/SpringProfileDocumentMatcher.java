package com.xylia.platform.events.profile;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.YamlProcessor;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Properties;

@Slf4j
public class SpringProfileDocumentMatcher implements YamlProcessor.DocumentMatcher, EnvironmentAware {

    private static final String[] DEFAULT_PROFILES = new String[]{"default"};
    private String[] activeProfiles = new String[0];

    public SpringProfileDocumentMatcher() {
    }

    public SpringProfileDocumentMatcher(String... activeProfiles) {
        addActiveProfiles(activeProfiles);
    }

    public void addActiveProfiles(String... profiles) {
        LinkedHashSet<String> profileSet = new LinkedHashSet<>(Arrays.asList(profiles));
        Collections.addAll(profileSet, profiles);

        this.activeProfiles = profileSet.toArray(new String[profileSet.size()]);
        log.info("Active profiles in the SDK:" + Arrays.toString(activeProfiles));
    }

    @Override
    public YamlProcessor.MatchStatus matches(Properties properties) {
        String[] profiles = this.activeProfiles;
        if (profiles.length == 0)
            profiles = DEFAULT_PROFILES;

        return new ArrayDocumentMatcher("spring.profiles", profiles).matches(properties);
    }

    @Override
    public void setEnvironment(Environment environment) {

    }
}
