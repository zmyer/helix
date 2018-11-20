package org.apache.helix.controller.stages;

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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

// TODO: 2018/6/15 by zmyer
public class ReadClusterDataStage extends AbstractBaseStage {
    private static final Logger logger = LoggerFactory.getLogger(ReadClusterDataStage.class.getName());

    private ClusterDataCache _cache = null;

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager == null) {
      throw new StageException("HelixManager attribute value is null");
    }

        ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());
        if (cache == null && _cache == null) {
            cache = new ClusterDataCache(event.getClusterName());
        }
        _cache = cache;

    HelixDataAccessor dataAccessor = manager.getHelixDataAccessor();
    _cache.refresh(dataAccessor);
    final ClusterConfig clusterConfig = cache.getClusterConfig();
    if (!_cache.isTaskCache()) {
      final ClusterStatusMonitor clusterStatusMonitor =
          event.getAttribute(AttributeName.clusterStatusMonitor.name());
      asyncExecute(_cache.getAsyncTasksThreadPool(), new Callable<Object>() {
        @Override public Object call() {
          // Update the cluster status gauges
          if (clusterStatusMonitor != null) {
            LogUtil.logDebug(logger, _eventId, "Update cluster status monitors");

                        Set<String> instanceSet = Sets.newHashSet();
                        Set<String> liveInstanceSet = Sets.newHashSet();
                        Set<String> disabledInstanceSet = Sets.newHashSet();
                        Map<String, Map<String, List<String>>> disabledPartitions = Maps.newHashMap();
                        Map<String, List<String>> oldDisabledPartitions = Maps.newHashMap();
                        Map<String, Set<String>> tags = Maps.newHashMap();
                        Map<String, LiveInstance> liveInstanceMap = _cache.getLiveInstances();
                        for (Map.Entry<String, InstanceConfig> e : _cache.getInstanceConfigMap().entrySet()) {
                            String instanceName = e.getKey();
                            InstanceConfig config = e.getValue();
                            instanceSet.add(instanceName);
                            if (liveInstanceMap.containsKey(instanceName)) {
                                liveInstanceSet.add(instanceName);
                            }
                            if (!config.getInstanceEnabled() || (clusterConfig.getDisabledInstances() != null
                                    && clusterConfig.getDisabledInstances().containsKey(instanceName))) {
                                disabledInstanceSet.add(instanceName);
                            }

                            // TODO : Get rid of this data structure once the API is removed.
                            oldDisabledPartitions.put(instanceName, config.getDisabledPartitions());
                            disabledPartitions.put(instanceName, config.getDisabledPartitionsMap());

              Set<String> instanceTags = Sets.newHashSet(config.getTags());
              tags.put(instanceName, instanceTags);
            }
            clusterStatusMonitor
                .setClusterInstanceStatus(liveInstanceSet, instanceSet, disabledInstanceSet,
                    disabledPartitions, oldDisabledPartitions, tags);
            LogUtil.logDebug(logger, _eventId, "Complete cluster status monitors update.");
          }
          return null;
        }
      });
    }
    event.addAttribute(AttributeName.ClusterDataCache.name(), _cache);
  }
}
