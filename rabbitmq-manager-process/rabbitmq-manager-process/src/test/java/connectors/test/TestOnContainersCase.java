package connectors.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.stream.Stream;

@Slf4j
public class TestOnContainersCase {

    protected static final RabbitMQContainer container = new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.8.0-management"));
    static {
        container.start();
        container.withExposedPorts(5672,5673,15672,15673);
        container.withAdminUser("guest");
        container.withAdminPassword("guest");
        container.withAccessToHost(true);
        container.withReuse(true);

        log.info("Container ip address: %s".formatted(container.getContainerIpAddress()));
        log.info("Container host: %s".formatted(container.getHost()));
        Stream.of(container.getAmqpsPort()).forEach(amqp -> log.info("AMQP port: %s".formatted(amqp)));
        container.getExposedPorts().forEach(exposedPort -> log.info("Exposed port: %s".formatted(exposedPort)));
    }

    @BeforeEach
    public void setUp() {
        Assertions.assertEquals(container.isRunning(), true, ()-> "Container is not running");
    }

    @AfterAll
    public static void stopContainer() {
        container.stop();
    }
}
