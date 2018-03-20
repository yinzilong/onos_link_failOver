/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.onosproject.segmentrouting;

import com.google.common.collect.ImmutableSet;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.intf.Interface;
import org.onosproject.net.intf.InterfaceServiceAdapter;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Mock Interface Service.
 */
public class MockInterfaceService extends InterfaceServiceAdapter {
    private Set<Interface> interfaces;

    MockInterfaceService(Set<Interface> interfaces) {
        this.interfaces = ImmutableSet.copyOf(interfaces);
    }

    @Override
    public Set<Interface> getInterfacesByPort(ConnectPoint cp) {
        return interfaces.stream().filter(intf -> cp.equals(intf.connectPoint()))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Interface> getInterfaces() {
        return interfaces;
    }
}
