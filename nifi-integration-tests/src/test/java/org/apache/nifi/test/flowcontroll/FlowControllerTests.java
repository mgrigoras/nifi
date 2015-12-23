package org.apache.nifi.test.flowcontroll;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.net.URL;
import java.util.Collections;
import java.util.UUID;

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.admin.service.UserService;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.repository.RingBufferEventRepository;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.test.s2s.SiteToSiteTests.SimpleProcessor;
import org.apache.nifi.test.utils.NiFiTestUtils;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FlowControllerTests {

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
        this.controller = FlowController.createStandaloneInstance(repository, NiFiProperties.getInstance(),
                mock(UserService.class), mock(AuditService.class), null);
    }

    @Test
    public void testControllerShutdown() throws Exception {
        ProcessGroup receiverGroup = controller.createProcessGroup("PG");
        NiFiTestUtils.setControllerRootGroup(this.controller, receiverGroup);
        ProcessorNode destination = this.controller.createProcessor(DummyProcessor.class.getName(), UUID.randomUUID().toString());
        destination.setAutoTerminatedRelationships(Collections.singleton(new Relationship.Builder().name("success").build()));
        receiverGroup.addProcessor(destination);
        this.controller.startProcessGroup(receiverGroup.getIdentifier());
        Thread.sleep(200); // give it some time to run
        this.controller.shutdown(false);
        assertTrue(controller.isTerminated());
    }

    public static class DummyProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
            // nothing to do intentionally
        }
    }
}
