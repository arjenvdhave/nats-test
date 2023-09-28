package nl.nats.test;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

class NatsContainer extends GenericContainer<NatsContainer> {

    private static final int BROKER_PORT = 4222;

    private static final int BROKER_HTTP_PORT = 8222;

    public static final String JETSTREAM_ENDPOINT = "/jsz";

    public static final String NATS_LATEST = "nats:2.9.22-alpine";

    public static final String NATS_LATEST_ALPINE = "nats:alpine";

    public NatsContainer(DockerImageName dockerImageName, String serverName, Network network) {
        super(dockerImageName);

        withExposedPorts(BROKER_PORT, BROKER_HTTP_PORT);
        withCopyFileToContainer(
                MountableFile.forClasspathResource("nl/nats/test/nats_test_config.conf"),
                "/config/config.conf");
        withImagePullPolicy(PullPolicy.alwaysPull());
        // withCommand("-js -m 8222 -c /config/config.conf -DVV"); // extra verbose debug logging
        withCommand(
                "-js -m 8222 -c /config/config.conf --cluster_name NATS --cluster nats://0.0.0.0:6222 --http_port 8222 --server_name "
                        + serverName);
        withNetwork(network);
        withNetworkAliases("nats");

        waitingFor(Wait.forHttp(JETSTREAM_ENDPOINT)
                       .forStatusCode(200)
                       .forPort(BROKER_HTTP_PORT));
        withLogConsumer((l) -> System.out.println("[NATS-test-container] -- " +
                l.getUtf8String()
                 .replaceAll("[\n\r]$", "")));
    }

    public String getNatsBrokerUrl() {
        return String.format("nats://%s:%s", getHost(), getMappedPort(BROKER_PORT));
    }

    public String getHttpServiceUrl() {
        return String.format("http://%s:%s", getHost(), getMappedPort(BROKER_HTTP_PORT));
    }
}
