package org.apache.nifi.spring;

import java.io.Closeable;
import java.util.Map;

import org.apache.nifi.util.Tuple;

public interface GenericSpringMessagingExchanger extends Closeable {

    <T> boolean send(T payload, Map<String, ?> headers, long timeout);

    <A> Tuple<A, Map<String, Object>> receive(long timeout);
}
