package org.carrot2.elasticsearch.plugin;

public class Preconditions {
    /**
     * Mark unreachable code path. Expected use scenario:
     * <pre>
     * throw Preconditions.unreachable();
     * </pre>
     */
    public RuntimeException unreachable() throws RuntimeException {
        throw new RuntimeException("Unreachable code assertion hit.");
    }
}
