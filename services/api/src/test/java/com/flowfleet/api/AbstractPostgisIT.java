package com.flowfleet.api;

import java.time.Duration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for API integration tests.
 *
 * <p>Uses the <em>singleton container</em> pattern: one PostGIS-enabled PostgreSQL is
 * started in a static initializer and shared by every {@code *IT} in the JVM. Because the
 * {@code @SpringBootTest} configuration is identical across subclasses, Spring reuses a
 * single application context bound to this one container — avoiding the classic pitfall
 * where a per-class {@code @Container} is recreated on a new port while the cached context
 * still points at the old one. Ryuk removes the container when the JVM exits.
 *
 * <p>If this passes, the schema, the PostGIS extension and the Spring Data JDBC mapping
 * are all genuinely consistent — not mocked.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractPostgisIT {

    // imresamu/postgis is a multi-arch rebuild of the official postgis image; unlike
    // postgis/postgis it publishes arm64, so it runs natively on Apple Silicon.
    static final DockerImageName POSTGIS_IMAGE =
            DockerImageName.parse("imresamu/postgis:16-3.5").asCompatibleSubstituteFor("postgres");

    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(POSTGIS_IMAGE)
            .withDatabaseName("flowfleet")
            .withUsername("flowfleet")
            .withPassword("flowfleet")
            // postgis/postgis is amd64-only; on an Apple-Silicon host it runs emulated and
            // first-boot can exceed the 60s default, especially on a busy machine.
            .withStartupTimeout(Duration.ofMinutes(3));

    static {
        POSTGRES.start();
    }

    @LocalServerPort
    protected int port;

    protected String url(String path) {
        return "http://localhost:" + port + path;
    }
}
