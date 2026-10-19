/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Guards the Jackson versions that end up on the runtime classpath.
 *
 * <p>Sova never calls Jackson itself, but {@code kafka-oauth-common} does — its {@code JSONUtil}
 * uses {@code ObjectMapper} — so Jackson ships in the distribution ZIP whenever the OAUTHBEARER
 * SASL path is used. Jackson requires {@code jackson-databind} and {@code jackson-core} to come
 * from the same release train; mixing them is unsupported and fails at runtime with
 * {@code NoSuchMethodError} rather than at build time.
 *
 * <p>A test-scoped dependency that pulls in an older Jackson can win Maven's nearest-wins
 * resolution and silently downgrade the runtime set. This test fails when that happens.
 */
class JacksonAlignmentTest {

    @Test
    void databindAndCoreComeFromTheSameReleaseTrain() {
        String databind = com.fasterxml.jackson.databind.cfg.PackageVersion.VERSION.toString();
        String core = com.fasterxml.jackson.core.json.PackageVersion.VERSION.toString();

        assertEquals(majorMinor(core), majorMinor(databind),
                "jackson-databind " + databind + " and jackson-core " + core
                        + " are from different release trains; check for a dependency "
                        + "downgrading Jackson via nearest-wins resolution");
    }

    private static String majorMinor(String version) {
        String[] parts = version.split("\\.");
        return parts[0] + "." + parts[1];
    }
}
