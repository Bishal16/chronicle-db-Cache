package com.telcobright.oltp.example;

/**
 * Launcher class for TestCasesGrpcClient with proper logging setup
 */
public class TestCasesGrpcClientLauncher {
    
    static {
        // Set up logging manager BEFORE any other classes are loaded
        System.setProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager");
        System.setProperty("org.jboss.logmanager.nocolor", "false");
    }
    
    public static void main(String[] args) {
        // Ensure logging is configured
        try {
            Class.forName("org.jboss.logmanager.LogManager");
        } catch (ClassNotFoundException e) {
            System.err.println("Warning: LogManager not found in classpath");
        }
        
        // Now call the actual client main method
        TestCasesGrpcClient.main(args);
    }
}