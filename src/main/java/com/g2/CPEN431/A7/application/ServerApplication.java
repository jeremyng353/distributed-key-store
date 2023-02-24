package com.g2.CPEN431.A7.application;

import ca.NetSysLab.ProtocolBuffers.Message;

public interface ServerApplication {
    Message.Msg handleRequest(Message.Msg request);
    Message.Msg generateAtCapacityError(Message.Msg request);
}
