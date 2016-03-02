package org.apache.nifi.spring;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.Tuple;

/**
 *
 */
@TriggerWhenEmpty
@Tags({"Spring", "Message", "Get", "Put", "Integration"})
public class SpringContextProcessor extends AbstractProcessor {

	public static final PropertyDescriptor CTX_CONFIG_NAME = new PropertyDescriptor.Builder()
            .name("Context Config file name")
            .description("The name of the Spring context configuration file")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	public static final PropertyDescriptor CTX_LIB_PATH = new PropertyDescriptor.Builder()
            .name("Context Libraries path")
            .description("Path to the directory with additional resources (i.e., JARs, configuration files etc.) to be added "
                       + "to the classpath.")
            .defaultValue(null)
            .addValidator(new ClientLibValidator())
            .required(true)
            .build();
	public static final PropertyDescriptor SEND_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Send Timeout")
            .description("Timeout for sending FlowFiles to Spring context. Default is infinite.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
	public static final PropertyDescriptor RECEIVE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Receive Timeout")
            .description("Timeout for receiving FlowFiles from Spring context. Default is infinite.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

	// ====

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are successfully converted to 'org.springframework.messaging.Message' "
            		   + "and sent to Spring context are routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be converted and/or sent to Spring context are routed to this relationship")
            .build();

    private final static Set<Relationship> relationships;

    private final static List<PropertyDescriptor> propertyDescriptors;

    // =======

    private volatile String applicationContextConfigFileName;

    private volatile String applicationContextLibPath;

    private volatile long sendTimeout;

    private volatile long receiveTimeout;

    private volatile GenericSpringMessagingExchanger exchnager;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(CTX_CONFIG_NAME);
        _propertyDescriptors.add(CTX_LIB_PATH);
        _propertyDescriptors.add(SEND_TIMEOUT);
        _propertyDescriptors.add(RECEIVE_TIMEOUT);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void start(ProcessContext processContext) {
    	this.applicationContextConfigFileName = processContext.getProperty(CTX_CONFIG_NAME).getValue();
    	this.applicationContextLibPath = processContext.getProperty(CTX_LIB_PATH).getValue();

    	String stStr = processContext.getProperty(SEND_TIMEOUT).getValue();
        this.sendTimeout = stStr == null ? 0 : FormatUtils.getTimeDuration(stStr, TimeUnit.MILLISECONDS);

    	String rtStr = processContext.getProperty(RECEIVE_TIMEOUT).getValue();
        this.receiveTimeout = rtStr == null ? 0 : FormatUtils.getTimeDuration(rtStr, TimeUnit.MILLISECONDS);

        try {
            URL[] additionalUrls = Utils.gatherAdditionalClassPathUrls(this.applicationContextLibPath);
            this.exchnager = SpringContextInitializer.createSpringContextDelegate(additionalUrls, this.applicationContextConfigFileName);
        } catch (Exception e) {
            throw new IllegalStateException("Failed while starting Spring Context", e);
        }
    }


    @OnStopped
    public void stop(ProcessContext processContext) {
        if (this.exchnager != null) {
            try {
                this.exchnager.close();
            } catch (IOException e) {
                getLogger().warn("Failed while closing Spring Context", e);
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession processSession) throws ProcessException {
        FlowFile flowFileToProcess = processSession.get();
        if (flowFileToProcess != null) {
            this.sendToSpring(flowFileToProcess, context, processSession);
        }
        this.receiveFromSpring(processSession);
    }

    /**
     *
     * @param flowFileToProcess
     * @param context
     * @param processSession
     */
    private void sendToSpring(FlowFile flowFileToProcess, ProcessContext context, ProcessSession processSession) {
        byte[] payload = this.extractMessage(flowFileToProcess, processSession);
        boolean sent = this.exchnager.send(payload, flowFileToProcess.getAttributes(), this.sendTimeout);
        if (sent) {
            processSession.getProvenanceReporter().send(flowFileToProcess, this.applicationContextConfigFileName);
            processSession.remove(flowFileToProcess);
        } else {
            this.transferToFailureAndReport(flowFileToProcess, context, processSession, null);
        }
    }

    /**
     *
     * @param processSession
     */
    private void receiveFromSpring(ProcessSession processSession) {
        final Tuple<?, Map<String, Object>> msgFromSpring = this.exchnager.receive(this.receiveTimeout);
        if (msgFromSpring != null) {
            FlowFile flowFileToProcess = processSession.create();
            flowFileToProcess = processSession.write(flowFileToProcess, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    Object payload = msgFromSpring.getKey();
                    if (payload instanceof byte[]) {
                        out.write((byte[]) payload);
                    } else {
                        throw new IllegalStateException("Failed while receiving message from Spring due to the "
                                + "payload type being other then byte[]. Please apply transformation/conversion on Spring side when ");
                    }
                }
            });
            flowFileToProcess = processSession.putAllAttributes(flowFileToProcess, this.extractFlowFileAttributesFromMessageHeaders(msgFromSpring.getValue()));
            this.transferToSuccessAndReport(flowFileToProcess, processSession);
        }
    }

    /**
     *
     * @param flowFileToProcess
     * @param processSession
     * @param e
     */
    private void transferToFailureAndReport(FlowFile flowFileToProcess, ProcessContext context, ProcessSession processSession, Exception e){
    	processSession.transfer(processSession.penalize(flowFileToProcess), REL_FAILURE);
    	if (e != null){
    		this.getLogger().error("Failed while sending FlowFile to SpringContext " + this.applicationContextConfigFileName, e);
    	} else {
    		this.getLogger().error("Timed out while sending FlowFile to SpringContext " + this.applicationContextConfigFileName);
    	}
        context.yield();
    }

    /**
     *
     * @param flowFileToProcess
     * @param processSession
     */
    private void transferToSuccessAndReport(FlowFile flowFileToProcess, ProcessSession processSession){
    	if (flowFileToProcess != null){
			processSession.transfer(flowFileToProcess, REL_SUCCESS);
            processSession.getProvenanceReporter().receive(flowFileToProcess, this.applicationContextConfigFileName);
		}
    }

    /**
     *
     * @param messageHeaders
     * @return
     */
    private Map<String, String> extractFlowFileAttributesFromMessageHeaders(Map<String, Object> messageHeaders) {
    	Map<String, String> attributes = new HashMap<>();
        for (Entry<String, Object> entry : messageHeaders.entrySet()) {
            if (entry.getValue() instanceof String) {
                attributes.put(entry.getKey(), (String) entry.getValue());
            }
        }
    	return attributes;
    }

    /**
     * Extracts contents of the {@link FlowFile} as byte array.
     */
    private byte[] extractMessage(FlowFile flowFile, ProcessSession processSession){
        final byte[] messageContent = new byte[(int) flowFile.getSize()];
        processSession.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, messageContent, true);
            }
        });
        return messageContent;
    }

    /**
     *
     */
   static class ClientLibValidator implements Validator {
       @Override
       public ValidationResult validate(String subject, String input, ValidationContext context) {
           String libDirPath = context.getProperty(CTX_LIB_PATH).getValue();
           StringBuilder invalidationMessageBuilder = new StringBuilder();
           if (libDirPath != null) {
               File file = new File(libDirPath);
               if (!file.isDirectory()) {
                   invalidationMessageBuilder.append("'ApplicationContext Libraries path' must point to a directory. Was '"
                           + file.getAbsolutePath() + "'.");
               }
           } else {
               invalidationMessageBuilder.append("'ApplicationContext Libraries path' must be provided. \n");
           }
           String invalidationMessage = invalidationMessageBuilder.toString();
           ValidationResult vResult;
           if (invalidationMessage.length() == 0) {
               vResult = new ValidationResult.Builder().subject(subject).input(input)
                       .explanation("Client lib path is valid and points to a directory").valid(true).build();
           } else {
               vResult = new ValidationResult.Builder().subject(subject).input(input)
                       .explanation("Client lib path is invalid. " + invalidationMessage)
                       .valid(false).build();
           }
           return vResult;
       }
   }
}
