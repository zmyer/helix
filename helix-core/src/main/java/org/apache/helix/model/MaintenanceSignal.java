package org.apache.helix.model;

import org.apache.helix.ZNRecord;

// TODO: 2018/7/25 by zmyer
public class MaintenanceSignal extends PauseSignal {
    public MaintenanceSignal(String id) {
        super(id);
    }

    public MaintenanceSignal(ZNRecord record) {
        super(record);
    }
}
