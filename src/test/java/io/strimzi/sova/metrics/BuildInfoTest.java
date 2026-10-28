/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics;

import java.util.Properties;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Unit tests for {@link BuildInfo}.
 */
class BuildInfoTest {
    private static Properties props(String version, String revision, String branch) {
        Properties props = new Properties();
        if (version != null) {
            props.setProperty("git.build.version", version);
        }
        if (revision != null) {
            props.setProperty("git.commit.id.abbrev", revision);
        }
        if (branch != null) {
            props.setProperty("git.branch", branch);
        }
        return props;
    }

    @Test
    void usesTheGeneratedValues() {
        BuildInfo info = BuildInfo.from(props("0.1.0", "a1b2c3d", "main"), "21.0.3");

        assertEquals(new BuildInfo("0.1.0", "a1b2c3d", "main", "21.0.3"), info);
    }

    @Test
    void missingKeysAreUnknown() {
        BuildInfo info = BuildInfo.from(new Properties(), null);

        assertEquals(new BuildInfo("unknown", "unknown", "unknown", "unknown"), info);
    }

    /**
     * Blank and missing values both become {@code unknown}, and either way the version is still
     * known: without a {@code .git} directory the plugin writes only the keys it can resolve.
     */
    @Test
    void blankAndMissingValuesAreUnknownButTheVersionIsStillKnown() {
        BuildInfo info = BuildInfo.from(props("0.1.0", "  ", null), "21.0.3");

        assertEquals(new BuildInfo("0.1.0", "unknown", "unknown", "21.0.3"), info);
    }

    /**
     * Reads the real {@code git.properties} from the build output, so a plugin that stops
     * writing the file, or renames its keys, fails here instead of silently turning every label
     * into {@code unknown}.
     */
    @Test
    void loadedGitPropertiesProvideTheVersion() {
        BuildInfo info = BuildInfo.load();

        assertNotEquals("unknown", info.version(),
                "git.properties was not found or has no git.build.version");
    }
}
