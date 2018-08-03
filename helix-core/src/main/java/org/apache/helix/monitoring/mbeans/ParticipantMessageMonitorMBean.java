package org.apache.helix.monitoring.mbeans;

import org.apache.helix.monitoring.SensorNameProvider;


// TODO: 2018/7/24 by zmyer
public interface ParticipantMessageMonitorMBean extends SensorNameProvider {
    public long getReceivedMessages();

    public long getDiscardedMessages();

    public long getCompletedMessages();

    public long getFailedMessages();

    public long getPendingMessages();
}