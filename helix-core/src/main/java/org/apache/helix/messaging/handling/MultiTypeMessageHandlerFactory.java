package org.apache.helix.messaging.handling;

import java.util.List;


// TODO: 2018/6/15 by zmyer
public interface MultiTypeMessageHandlerFactory extends MessageHandlerFactory {

    List<String> getMessageTypes();

}
