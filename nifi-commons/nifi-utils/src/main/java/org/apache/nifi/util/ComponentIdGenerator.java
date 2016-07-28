/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.util;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * IMPORTANT: This component is not part of public API!
 * ====================================================
 * <p>
 * This component generates type-one UUID. It is used for generating ID of all
 * NiFi components. Giving the 128-bit UUID structure which consists of Least
 * Significant Bits (LSB) and Most Significant Bits (MSB) this component
 * provides support for generating and maintaining INCEPTION ID of the component
 * (MSB) as well as its INSTANCE ID (LSB).
 * </p>
 * <p>
 * It is also important to understand that while this component does seed itself
 * from current time which could be extracted from the resulting UUID via call
 * to {@link UUID#timestamp()} operation, one should not be relying on such time
 * as the exact time when such ID was generated since in the event the same time
 * is passed to one of the {@link #generateId()} operation it will be
 * incremented by 1 since the goal of this component to only ensure uniqueness
 * and type-one semantics where each UUID generated by this component is
 * comparable and each subsequent ID is > then previous ID.
 * </p>
 * <p>
 * For more details on how it is interacted with please see
 * org.apache.nifi.web.util.SnippetUtils as well as
 * org.apache.nifi.web.util.SnippetUtilsTest which contain additional
 * documentation on its usage as well as ID generation contracts defined in
 * NiFi.
 * </p>
 */
public class ComponentIdGenerator {

    public static final Object lock = new Object();

    private static long lastTime;
    private static long clockSequence = 0;
    private static final SecureRandom randomGenerator = new SecureRandom();

    /**
     * Will generate unique time based UUID where the next UUID is always
     * greater then the previous.
     */
    public final static UUID generateId() {
        return generateId(System.currentTimeMillis());
    }

    /**
     *
     */
    public final static UUID generateId(long currentTime) {
        return generateId(currentTime, randomGenerator.nextLong());
    }

    /**
     *
     */
    public final static UUID generateId(long msb, long lsb) {
        long time;

        synchronized (lock) {
            if (msb <= lastTime) {
                msb = ++lastTime;
            }
            lastTime = msb;
        }

        time = msb;

        // low Time
        time = msb << 32;

        // mid Time
        time |= ((msb & 0xFFFF00000000L) >> 16);

        // hi Time
        time |= 0x1000 | ((msb >> 48) & 0x0FFF);

        long clockSequenceHi = clockSequence;
        clockSequenceHi <<= 48;
        lsb = clockSequenceHi | lsb;
        return new UUID(time, lsb);
    }
}