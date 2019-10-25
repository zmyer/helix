package org.apache.helix.model;

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

import java.util.Map;

import org.apache.helix.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created with IntelliJ IDEA.
 * User: zzhang
 * Date: 3/19/13
 * Time: 5:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestInstanceConfig {
  @Test
  public void testNotCheckingHostPortExistence() {
    InstanceConfig config = new InstanceConfig("node_0");
    Assert.assertTrue(config.isValid(),
        "HELIX-65: should not check host/port existence for instance-config");
  }

  @Test
  public void testGetParsedDomain() {
    InstanceConfig instanceConfig = new InstanceConfig(new ZNRecord("id"));
    instanceConfig.setDomain("cluster=myCluster,zone=myZone1,rack=myRack,host=hostname,instance=instance001");

    Map<String, String> parsedDomain = instanceConfig.getDomainAsMap();
    Assert.assertEquals(parsedDomain.size(), 5);
    Assert.assertEquals(parsedDomain.get("zone"), "myZone1");
  }

  @Test
  public void testGetParsedDomain_emptyDomain() {
    InstanceConfig instanceConfig = new InstanceConfig(new ZNRecord("id"));

    Map<String, String> parsedDomain = instanceConfig.getDomainAsMap();
    Assert.assertTrue(parsedDomain.isEmpty());
  }
}
