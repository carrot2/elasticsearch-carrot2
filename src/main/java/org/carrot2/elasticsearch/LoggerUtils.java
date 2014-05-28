package org.carrot2.elasticsearch;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;
import org.carrot2.core.ProcessingComponentSuite;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

final class LoggerUtils {
    private static Appender quietAppender = NullAppender.getNullAppender();

    /**
     * Set additivity of certain loggers to a given value.
     */
    static <T> T turnOffAdditivity(Callable<T> callable, Logger... loggers) throws Exception {
        boolean [] additivities = new boolean[loggers.length];
        for (int i = 0; i < loggers.length; i++) {
            if (loggers[i] != null) { 
                additivities[i] = loggers[i].getAdditivity();
                loggers[i].setAdditivity(false);
            }
        }
        try {
            return callable.call();
        } finally {
            for (int i = 0; i < loggers.length; i++) {
                if (loggers[i] != null) { 
                    loggers[i].setAdditivity(additivities[i]);
                }
            }
        }
    }

    static ProcessingComponentSuite quietCall(
            Callable<ProcessingComponentSuite> callable, Logger... loggers) throws Exception {
        for (int i = 0; i < loggers.length; i++) {
            if (loggers[i] != null) loggers[i].addAppender(quietAppender);
        }
        try {
            return turnOffAdditivity(callable, loggers);
        } finally {
            for (int i = 0; i < loggers.length; i++) {
                if (loggers[i] != null) loggers[i].removeAppender(quietAppender);
            }
        }
    }

    static void emitErrorResponse(RestChannel channel, 
                                  RestRequest request, 
                                  ESLogger logger,
                                  Throwable t) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, t));
        } catch (IOException e) {
            logger.error("Failed to send failure response.", e);
        }
    }
}
