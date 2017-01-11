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

    public static <T> T checkNotNull(T object) throws RuntimeException {
        if (object != null)
            return object;

        throw new IllegalArgumentException("Cannot be null");
    }
}
