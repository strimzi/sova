/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.http;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

/**
 * Log4j2 appender that records what a given class logged, so a test can assert on it.
 *
 * <p>Needed because some behaviour is only observable through the log — a Prometheus scrape that
 * fails answers the client with a generic 500 by design, and the log is the only place the cause
 * appears.
 */
final class CapturingAppender extends AbstractAppender {

    private final List<String> messages = new CopyOnWriteArrayList<>();
    private final Logger logger;

    private CapturingAppender(Logger logger) {
        super("capturing-" + logger.getName(), null, null, true, Property.EMPTY_ARRAY);
        this.logger = logger;
    }

    /**
     * Starts capturing everything logged by {@code type}'s logger.
     *
     * @param type the class whose logger to attach to
     * @return the attached appender; call {@link #detach()} when done
     */
    static CapturingAppender attachedTo(Class<?> type) {
        Logger logger = (Logger) LogManager.getLogger(type);
        CapturingAppender appender = new CapturingAppender(logger);
        appender.start();
        logger.addAppender(appender);
        return appender;
    }

    @Override
    public void append(LogEvent event) {
        messages.add(event.getMessage().getFormattedMessage());
    }

    /**
     * @return the messages logged since attachment
     */
    List<String> messages() {
        return List.copyOf(messages);
    }

    /**
     * Removes this appender from its logger.
     */
    void detach() {
        logger.removeAppender(this);
        stop();
    }
}
