package com.xylia.platform.events.profile;

import org.springframework.beans.factory.config.YamlProcessor;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

public class ArrayDocumentMatcher implements YamlProcessor.DocumentMatcher {

    private final String key;

    private final String[] patterns;

    public ArrayDocumentMatcher(final String key, final String... patterns) {
        this.key = key;
        Object[] original;
        this.patterns = copyOf(patterns);
    }

    @Override
    public YamlProcessor.MatchStatus matches(Properties properties) {

        if (!properties.containsKey(key))
            return YamlProcessor.MatchStatus.ABSTAIN;

        Set<String> values = StringUtils.commaDelimitedListToSet(properties.getProperty(this.key));

        for (String pattern : this.patterns) {
            for (String value : values) {
                if (value.matches(pattern))
                    return YamlProcessor.MatchStatus.FOUND;
            }
        }
        return YamlProcessor.MatchStatus.NOT_FOUND;
    }


    private static final String[] copyOf(String[] sourceArray) {
        if (sourceArray == null)
            return sourceArray;
        else
            return Arrays.copyOf(sourceArray, sourceArray.length);
    }
}
