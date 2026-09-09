package com.flowfleet.flink.sink;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.stream.Collectors;

/** Runs a {@code ;}-separated .sql script against a JDBC URL — for standing up sink schemas in tests. */
final class Sql {

    private Sql() {}

    static void runScript(String jdbcUrl, String user, String password, Path scriptFile) {
        String raw;
        try {
            raw = Files.readString(scriptFile);
        } catch (Exception e) {
            throw new IllegalStateException("cannot read " + scriptFile, e);
        }
        // drop full-line -- comments, then split on ';'
        String noComments = raw.lines()
                .filter(l -> !l.strip().startsWith("--"))
                .collect(Collectors.joining("\n"));

        try (Connection c = DriverManager.getConnection(jdbcUrl, user, password);
             Statement s = c.createStatement()) {
            for (String stmt : Arrays.stream(noComments.split(";"))
                    .map(String::trim).filter(x -> !x.isEmpty()).toList()) {
                s.execute(stmt);
            }
        } catch (Exception e) {
            throw new IllegalStateException("script failed: " + e.getMessage(), e);
        }
    }

    static long count(String jdbcUrl, String user, String password, String query) {
        try (Connection c = DriverManager.getConnection(jdbcUrl, user, password);
             Statement s = c.createStatement();
             var rs = s.executeQuery(query)) {
            rs.next();
            return rs.getLong(1);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
