package org.carrot2.elasticsearch;

/* */
final class Preconditions {
    /**
     * Mark unreachable code path. Expected use scenario:
     * <pre>
     * throw Preconditions.unreachable();
     * </pre>
     */
    public static RuntimeException unreachable() throws RuntimeException {
        throw new RuntimeException("Unreachable code assertion hit.");
    }
}
