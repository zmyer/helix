package org.apache.helix.monitoring.mbeans;

import org.apache.helix.monitoring.SensorNameProvider;

// TODO: 2018/7/24 by zmyer
public interface ThreadPoolExecutorMonitorMBean extends SensorNameProvider {
    int getThreadPoolCoreSizeGauge();

    int getThreadPoolMaxSizeGauge();

    int getNumOfActiveThreadsGauge();

    int getQueueSizeGauge();
}
