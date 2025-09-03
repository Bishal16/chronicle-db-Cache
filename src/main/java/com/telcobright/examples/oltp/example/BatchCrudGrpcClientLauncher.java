package com.telcobright.examples.oltp.example;

/**
 * Launcher class that sets up JVM properties before loading the actual client.
 * This ensures logging is properly configured before any static initializers run.
 */
public class BatchCrudGrpcClientLauncher {
    
    static {
        // Set up logging manager BEFORE any other classes are loaded
        System.setProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager");
        System.setProperty("org.jboss.logmanager.nocolor", "false");
    }
    
    public static void main(String[] args) {
        // Ensure logging is configured
        try {
            // Force load the LogManager class
            Class.forName("org.jboss.logmanager.LogManager");
        } catch (ClassNotFoundException e) {
            System.err.println("Warning: LogManager not found in classpath");
        }
        
        // Now call the actual client main method
        BatchCrudGrpcClient.main(args);
    }
}