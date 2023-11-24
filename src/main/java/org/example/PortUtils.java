package org.example;

public final class PortUtils {

    private static final PortAllocator portAllocator = new PortAllocator();

    /**
     * Find consecutive open ports, returning the first one in the range.
     */
    public static int findOpenPorts(int count) {
        return portAllocator.getPorts(count);
    }

    /**
     * Find open port.
     */
    public static int findOpenPort() {
        return portAllocator.getPort();
    }

    public static int findBrokerPort() {
        return findOpenPorts(5);
    }
}
