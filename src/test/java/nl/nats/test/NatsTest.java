package nl.nats.test;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.NatsMessage;
import org.junit.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.fail;

public class NatsTest {

    private static final String STREAM_NAME = "test_stream";
    private static final String DURABLE_NAME = "durable_test";
    private static final String SUBJECT = "test.subject";
    private static final String QUEUE_GROUP = "qgroup";


    private static final NatsContainer natsContainer1;

    private static final NatsContainer natsContainer2;

    private static final NatsContainer natsContainer3;


    static {
        var network = Network.newNetwork();
        natsContainer1 = new NatsContainer(DockerImageName.parse(NatsContainer.NATS_LATEST),
                "nats-cluster-server1", network);
        natsContainer1.start();

        natsContainer2 = new NatsContainer(DockerImageName.parse(NatsContainer.NATS_LATEST),
                "nats-cluster-server2", network);
        natsContainer2.start();

        natsContainer3 = new NatsContainer(DockerImageName.parse(NatsContainer.NATS_LATEST),
                "nats-cluster-server3", network);
        natsContainer3.start();
    }

    private JetStream jetStream;
    private Connection connection;

    private CountDownLatch latch;

    public void setup() throws IOException, InterruptedException, JetStreamApiException {
        connection = Nats.connect(Options.builder()
                                         .server(natsContainer1.getNatsBrokerUrl())
                                         .authHandler(Nats.credentials(getClass().getResource("test-user.creds")
                                                                                 .getPath()))
                                         .build());
        jetStream = connection.jetStream();
        JetStreamManagement mngmt = connection.jetStreamManagement();

        mngmt.addStream(StreamConfiguration.builder(StreamConfiguration.builder()
                                                                       .name(STREAM_NAME)
                                                                       .subjects(SUBJECT)
                                                                       .storageType(StorageType.Memory)
                                                                       .retentionPolicy(RetentionPolicy.WorkQueue)
                                                                       .build())
                                           .build());
    }

    @Test
    public void testWithPull() throws InterruptedException, IOException, JetStreamApiException {
        setup();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        startBackgroundPull(executor, 3);

        latch = new CountDownLatch(100);
        try {
            for (int i = 0; i < 100; i++) {
                jetStream.publish(NatsMessage.builder()
                                             .subject(SUBJECT)
                                             .data("Content" + i)
                                             .build());
            }

        } catch (IOException | JetStreamApiException e) {
            throw new RuntimeException(e);
        }

        if (!latch.await(1, TimeUnit.MINUTES))
            fail("Timeout");
    }

    @Test
    public void testWithPush() throws InterruptedException, IOException, JetStreamApiException {
        setup();
        jetStream.subscribe(SUBJECT, QUEUE_GROUP, connection.createDispatcher(),
                this::handleMsg, true, PushSubscribeOptions.builder()
                                                           .stream(STREAM_NAME)
                                                           .build());

        latch = new CountDownLatch(100);
        try {
            for (int i = 0; i < 100; i++) {
                jetStream.publish(NatsMessage.builder()
                                             .subject(SUBJECT)
                                             .data("Content" + i)
                                             .build());
            }

        } catch (IOException | JetStreamApiException e) {
            throw new RuntimeException(e);
        }


        if (!latch.await(1, TimeUnit.MINUTES))
            fail("Timeout");
    }

    public void startBackgroundPull(ExecutorService executorService, int numberOfThreads) {

        for (int i = 0; i < numberOfThreads; i++) {
            executorService.execute(() -> {
                boolean running = true;
                while (running) {
                    try {
                        pullForNextMessage(1);
                    } catch (InterruptedException e) {
                        running = false;
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

    }

    private void pullForNextMessage(int batchSize) throws InterruptedException {
        try {
            FetchConsumer fetchConsumer = getContext().fetch(FetchConsumeOptions.builder()
                                                                                .maxMessages(batchSize)
                                                                                .expiresIn(-1)
                                                                                .build());
            for (int i = 0; i < batchSize; i++) {
                Message msg = fetchConsumer.nextMessage();
                if (msg == null)
                    return;

                handleMsg(msg);
                msg.ack();
            }
        } catch (IOException | JetStreamApiException | JetStreamStatusCheckedException e) {
            e.printStackTrace();
        }
    }

    private ConsumerContext getContext() throws JetStreamApiException, IOException {
        var streamContext = this.jetStream.streamContext(STREAM_NAME);
//        try {
//            return streamContext.consumerContext(DURABLE_NAME);
//        } catch (Exception e) {
//        }

        return streamContext.createOrUpdateConsumer(ConsumerConfiguration.builder()
                                                                         .durable(DURABLE_NAME)
                                                                         .ackWait(Duration.ofSeconds(30))
                                                                         .filterSubject(SUBJECT)
                                                                         .build());
    }

    private void handleMsg(Message msg) {
        System.out.println("Received msg" + msg.toString());
        latch.countDown();
    }

}
