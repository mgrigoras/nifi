package org.apache.nifi.test.utils;

import java.lang.reflect.Method;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.groups.ProcessGroup;

public class NiFiTestUtils {

    public static void setControllerRootGroup(FlowController controller, ProcessGroup processGroup) {
        try {
            Method m = FlowController.class.getDeclaredMethod("setRootGroup", ProcessGroup.class);
            m.setAccessible(true);
            m.invoke(controller, processGroup);
            controller.initializeFlow();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set root group", e);
        }
    }
}
