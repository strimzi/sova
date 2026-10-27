/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Build metadata exposed through {@code sova_build_info}.
 *
 * <p>Version, revision and branch come from the {@code git.properties} resource written by the
 * git-commit-id Maven plugin. The Java version is read from the running JVM.
 *
 * <p>Any field that cannot be determined is {@value #UNKNOWN}. That includes the revision and
 * branch when the build ran without a {@code .git} directory (a source tarball, for example):
 * the plugin then leaves those keys out of the file.
 *
 * @param version     Maven project version (e.g. {@code 0.1.0})
 * @param revision    git commit SHA abbreviation (e.g. {@code a1b2c3d})
 * @param branch      git branch name (e.g. {@code main})
 * @param javaVersion version of the running JVM
 */
public record BuildInfo(String version, String revision, String branch, String javaVersion) {
    /** Value used for any field that cannot be determined. */
    static final String UNKNOWN = "unknown";

    private static final Logger LOG = LoggerFactory.getLogger(BuildInfo.class);

    /** Classpath location of the file written by the git-commit-id Maven plugin. */
    private static final String GIT_PROPERTIES = "git.properties";

    /**
     * Reads the build metadata from the classpath and the running JVM.
     *
     * @return the build info; every field falls back to {@value #UNKNOWN} on error
     */
    public static BuildInfo load() {
        Properties props = new Properties();
        try (InputStream is = BuildInfo.class.getClassLoader().getResourceAsStream(GIT_PROPERTIES)) {
            if (is != null) {
                props.load(is);
            } else {
                LOG.warn("{} not found on classpath — build info labels will be '{}'",
                        GIT_PROPERTIES, UNKNOWN);
            }
        } catch (IOException e) {
            LOG.warn("Failed to load {}", GIT_PROPERTIES, e);
        }
        return from(props, System.getProperty("java.version"));
    }

    /**
     * Builds the info from already-loaded properties.
     *
     * @param props       the contents of {@code git.properties}
     * @param javaVersion version of the running JVM, may be {@code null}
     * @return the build info; every missing or blank field is {@value #UNKNOWN}
     */
    static BuildInfo from(Properties props, String javaVersion) {
        return new BuildInfo(
                valueOrUnknown(props.getProperty("git.build.version")),
                valueOrUnknown(props.getProperty("git.commit.id.abbrev")),
                valueOrUnknown(props.getProperty("git.branch")),
                valueOrUnknown(javaVersion));
    }

    private static String valueOrUnknown(String value) {
        return (value == null || value.isBlank()) ? UNKNOWN : value;
    }

    @Override
    public String toString() {
        return "BuildInfo{" +
            "\n\tversion='" + version + '\'' +
            "\n\trevision='" + revision + '\'' +
            "\n\tbranch='" + branch + '\'' +
            "\n\tjavaVersion='" + javaVersion + '\'' +
            '}';
    }
}
