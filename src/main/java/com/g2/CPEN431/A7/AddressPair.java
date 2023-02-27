package com.g2.CPEN431.A7;

public class AddressPair {
    private String ip;
    private int port;

    public AddressPair(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }
}
