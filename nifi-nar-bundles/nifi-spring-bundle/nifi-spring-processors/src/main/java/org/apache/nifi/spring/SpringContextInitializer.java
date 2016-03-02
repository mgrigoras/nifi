package org.apache.nifi.spring;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.commons.io.IOUtils;

class SpringContextInitializer {

    private static final String SC_DELEGATE_NAME = "org.apache.nifi.spring.bootstrap.SpringContextDelegate";

    public static GenericSpringMessagingExchanger createSpringContextDelegate(URL[] classpath, String config) {
        @SuppressWarnings("resource") // will be closed when context is closed
        SpringContextClassLoader contextCl = new SpringContextClassLoader(classpath, SpringContextInitializer.class.getClassLoader());
        try {
            InputStream delegateStream = contextCl.getResourceAsStream(SC_DELEGATE_NAME.replace('.', '/') + ".class");
            byte[] delegateBytes = IOUtils.toByteArray(delegateStream);
            Class<?> clazz = contextCl.doDefineClass(SC_DELEGATE_NAME, delegateBytes, 0, delegateBytes.length);
            Constructor<?> ctr = clazz.getDeclaredConstructor(String.class);
            ctr.setAccessible(true);
            return (GenericSpringMessagingExchanger) ctr.newInstance(config);
        } catch (Exception e) {
            throw new IllegalStateException("Failed", e);
        }
    }

    /**
     *
     */
    private static class SpringContextClassLoader extends URLClassLoader {

        public SpringContextClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        public final Class<?> doDefineClass(String name, byte[] b, int off, int len) {
            return this.defineClass(name, b, off, len);
        }
    }
}
