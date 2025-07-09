package com.telcobright.oltp.queue.chronicle;

import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;

@ApplicationScoped
@Startup
public class ChronicleJvmArgsVerifier {

    private static final List<String> REQUIRED_ARGS = Arrays.asList(
            "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
            "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED",
            "--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
            "--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED"
    );

    @PostConstruct
    public void verifyJvmArgs() {
        List<String> actualJvmArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();

        boolean missingArgs = REQUIRED_ARGS.stream()
                .anyMatch(requiredArg -> actualJvmArgs.stream().noneMatch(actual -> actual.contains(requiredArg)));

        if (missingArgs) {
            System.err.println("\n=========== JVM ARG CHECK FAILED ===========");
            System.err.println("Missing required JVM arguments for Chronicle Queue compatibility!");
            System.err.println("Expected to find each of the following:");
            REQUIRED_ARGS.forEach(System.err::println);
            System.err.println("\nActual JVM Args:");
            actualJvmArgs.forEach(System.err::println);

            throw new RuntimeException("Missing required JVM module opens/exports for Chronicle Queue.");
        } else {
            System.out.println("\n✅ Chronicle JVM argument check PASSED.");
        }
    }
}
