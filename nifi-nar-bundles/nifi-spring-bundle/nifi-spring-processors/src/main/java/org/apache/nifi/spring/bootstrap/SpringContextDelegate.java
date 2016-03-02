package org.apache.nifi.spring.bootstrap;

import java.io.Closeable;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.Map;

import org.apache.nifi.spring.GenericSpringMessagingExchanger;
import org.apache.nifi.spring.SpringNiFiConstants;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.MessageBuilder;

class SpringContextDelegate implements Closeable, GenericSpringMessagingExchanger {

    private final Logger logger = LoggerFactory.getLogger(SpringContextDelegate.class);

    private final ClassPathXmlApplicationContext applicationContext;

    private final MessageChannel toSpringChannel;

    private final PollableChannel fromSpringChannel;

    private SpringContextDelegate(String configName) {
        ClassLoader orig = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        if (logger.isDebugEnabled()) {
            logger.debug("Using " + Thread.currentThread().getContextClassLoader()
                    + " as context class loader while loading Spring Context '" + configName + "'.");
        }
        try {
            this.applicationContext = new ClassPathXmlApplicationContext(configName);
            if (this.applicationContext.containsBean(SpringNiFiConstants.FROM_NIFI)){
                this.toSpringChannel = this.applicationContext.getBean(SpringNiFiConstants.FROM_NIFI, MessageChannel.class);
            } else {
                this.toSpringChannel = null;
            }
            if (this.applicationContext.containsBean(SpringNiFiConstants.TO_NIFI)){
                this.fromSpringChannel = this.applicationContext.getBean(SpringNiFiConstants.TO_NIFI, PollableChannel.class);
            } else {
                this.fromSpringChannel = null;
            }
        } finally {
            Thread.currentThread().setContextClassLoader(orig);
        }
    }

    @Override
    public <T> boolean send(T payload, Map<String, ?> messageHeaders, long timeout) {
        if (this.toSpringChannel != null){
            return this.toSpringChannel.send(MessageBuilder.withPayload(payload).copyHeaders(messageHeaders).build(), timeout);
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <A> Tuple<A, Map<String, Object>> receive(long timeout) {
        if (this.fromSpringChannel != null) {
            Message<?> message = this.fromSpringChannel.receive(timeout);
            if (message != null) {
                Map<String, Object> messageHeaders = message.getHeaders();
                return new Tuple<>((A) message.getPayload(), messageHeaders);
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        logger.info("Closing ApplicationContext");
        this.applicationContext.close();
        logger.info("Closing " + this.getClass().getClassLoader());
        ((URLClassLoader) this.getClass().getClassLoader()).close();
    }
}
