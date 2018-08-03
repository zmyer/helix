package org.apache.helix.common;

import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;

/**
 * A generic extended single-thread class to handle ClusterEvent (multiple-producer/single consumer
 * style).
 *
 * This class is deprecated, please use {@link org.apache.helix.common.DedupEventProcessor}.
 */
// TODO: 2018/6/4 by zmyer
@Deprecated
public abstract class ClusterEventProcessor
        extends DedupEventProcessor<ClusterEventType, ClusterEvent> {

    public ClusterEventProcessor(String clusterName) {
        this(clusterName, "Helix-ClusterEventProcessor");
    }

    public ClusterEventProcessor(String clusterName, String processorName) {
        super(clusterName, processorName);
    }

    // TODO: 2018/7/25 by zmyer
    public void queueEvent(ClusterEvent event) {
        _eventQueue.put(event.getEventType(), event);
    }
}
