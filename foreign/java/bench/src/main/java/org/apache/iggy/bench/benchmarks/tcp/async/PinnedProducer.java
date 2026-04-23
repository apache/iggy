/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iggy.bench.benchmarks.tcp.async;

import org.apache.iggy.bench.models.cli.GlobalCliArgs;
import org.apache.iggy.bench.models.cli.PinnedProducerCliArgs;
import org.apache.iggy.bench.models.provision.ProvisionedResources;
import org.apache.iggy.bench.provision.ResourceProvisioner;

public final class PinnedProducer {

    private final GlobalCliArgs globalCliArgs;
    private final PinnedProducerCliArgs pinnedProducerCliArgs;
    private final ResourceProvisioner resourceProvisioner;

    PinnedProducer(
            GlobalCliArgs globalCliArgs,
            PinnedProducerCliArgs pinnedProducerCliArgs,
            ResourceProvisioner resourceProvisioner) {
        this.globalCliArgs = globalCliArgs;
        this.pinnedProducerCliArgs = pinnedProducerCliArgs;
        this.resourceProvisioner = resourceProvisioner;
    }

    public PinnedProducer(GlobalCliArgs globalCliArgs, PinnedProducerCliArgs pinnedProducerCliArgs) {
        this(globalCliArgs, pinnedProducerCliArgs, new ResourceProvisioner());
    }

    public ProvisionedResources provisionResources() {
        return resourceProvisioner.provisionResources(globalCliArgs, pinnedProducerCliArgs);
    }

    public void run() {}
}
