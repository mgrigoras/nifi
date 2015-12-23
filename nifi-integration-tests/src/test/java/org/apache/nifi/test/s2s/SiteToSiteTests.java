package org.apache.nifi.test.s2s;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.admin.service.UserService;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.repository.RingBufferEventRepository;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.s2s.SiteToSiteSender;
import org.apache.nifi.test.utils.NiFiTestUtils;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SiteToSiteTests {

    private final UserService userService = mock(UserService.class);
    private final AuditService auditService = mock(AuditService.class);

    private FlowController controller;

    @BeforeClass
    public static void beforeClass() {
        try {
            URL url = ClassLoader.getSystemClassLoader().getResource("nifi.properties");
            System.setProperty("nifi.properties.file.path", url.getFile());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to discover nifi.properties at the root of the classpath", e);
        }
    }

    @Before
    public void before() {
        RingBufferEventRepository repository = new RingBufferEventRepository(1);
        this.controller = FlowController
                .createStandaloneInstance(repository, NiFiProperties.getInstance(), this.userService, this.auditService, null);
    }

    @After
    public void after() {
        this.controller.shutdown(false);
    }

    /**
     * Demonstrates normal operation of sending to RPG.
     * IMPORTANT: It periodically fails with the following message:
     * Initiated graceful shutdown of flow controller...waiting up to 10 seconds
     *      2015-12-23 11:31:46,149 WARN [main] o.apache.nifi.controller.FlowController Controller hasn't terminated properly.
     *      There exists an uninterruptable thread that will take an indeterminate amount of time to stop.  Might need to kill the program manually.
     * Also, some times it fails with data loss even though FlowController shows that it has been terminated successfully
     * There appears to be some race condition between sending and initiating controller stop which results in data loss
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testSendToSiteNormalOperation() throws Exception {
        String portId = UUID.randomUUID().toString();
        ProcessGroup receiverGroup = controller.createProcessGroup("SITE");
        NiFiTestUtils.setControllerRootGroup(this.controller, receiverGroup);

        // SOURCE
        ProcessorNode destination = controller.createProcessor(SimpleProcessor.class.getName(),
                UUID.randomUUID().toString());
        SimpleProcessor processsor = (SimpleProcessor) destination.getProcessor();
        destination.setAutoTerminatedRelationships(Collections.singleton(new Relationship.Builder().name("success").build()));
        receiverGroup.addProcessor(destination);
        destination.setProcessGroup(receiverGroup);

        // PORT
        Port port = controller.createRemoteInputPort(portId, "in");
        receiverGroup.addInputPort(port);
        port.setProcessGroup(receiverGroup);

        // CONNECTION
        Connection connection = this.controller.createConnection(UUID.randomUUID().toString(), "Connection to SITE", port,
                destination, Collections.<String> emptyList());
        receiverGroup.addConnection(connection);
        connection.setProcessGroup(receiverGroup);

        // START RPG
        receiverGroup.startProcessing();

        // RPG SENDER
        SiteToSiteClientConfig s2sConfig = new SiteToSiteClient.Builder()
                .portIdentifier(portId)
                .buildConfig();
        SiteToSiteSender sender = new SiteToSiteSender(s2sConfig);

        // Send data
        int sendCount = 10;
        for (int i = 0; i < sendCount; i++) {
            String s = new String("hello");
            sender.send(s.getBytes(), Collections.EMPTY_MAP);
        }
        sender.close();
        assertEquals(sendCount, processsor.getInvocationCount());
    }

    /**
     * Processor that transfers received FF to SUCCESS relationship
     */
    public static class SimpleProcessor extends AbstractProcessor {
        private final AtomicInteger counter = new AtomicInteger();

        public static final Relationship REL_SUCCESS = new Relationship.Builder()
                .name("success")
                .description("success")
                .build();

        private final Set<Relationship> relationships;

        public SimpleProcessor() {
            final Set<Relationship> rels = new HashSet<>();
            rels.add(REL_SUCCESS);
            this.relationships = Collections.unmodifiableSet(rels);
        }

        public int getInvocationCount() {
            return this.counter.get();
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
            FlowFile flowFile = session.get();
            if (flowFile != null) {
                this.counter.incrementAndGet();
                session.transfer(flowFile, REL_SUCCESS);
            }
        }

        @Override
        public Set<Relationship> getRelationships() {
            return this.relationships;
        }
    }
}
