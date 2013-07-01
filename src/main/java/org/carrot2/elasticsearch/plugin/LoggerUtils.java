package org.carrot2.elasticsearch.plugin;

import java.util.concurrent.Callable;

import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;
import org.carrot2.core.ProcessingComponentSuite;

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

    public static ProcessingComponentSuite quietCall(
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
}
