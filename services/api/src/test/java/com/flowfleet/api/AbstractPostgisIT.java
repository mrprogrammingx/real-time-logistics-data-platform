package com.flowfleet.api;

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

    static final DockerImageName POSTGIS_IMAGE =
            DockerImageName.parse("postgis/postgis:16-3.4").asCompatibleSubstituteFor("postgres");

    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(POSTGIS_IMAGE)
            .withDatabaseName("flowfleet")
            .withUsername("flowfleet")
            .withPassword("flowfleet");

    static {
        POSTGRES.start();
    }

    @LocalServerPort
    protected int port;

    protected String url(String path) {
        return "http://localhost:" + port + path;
    }
}
