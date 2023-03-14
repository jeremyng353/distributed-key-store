package com.g2.CPEN431.A7;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

public class AddressPair {
    private String ip;
    private int port;

    public AddressPair(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public AddressPair(String addressAndPort) {
        String[] addressAndPortSplit = addressAndPort.split(":");
        this.ip = addressAndPortSplit[0];
        this.port = Integer.parseInt(addressAndPortSplit[1]);
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AddressPair that = (AddressPair) o;
        return port == that.port && Objects.equals(ip, that.ip);
    }

    @Override
    public int hashCode() {
        try {
            return new BigInteger(MessageDigest.getInstance("SHA-256").digest((ip + port).getBytes(StandardCharsets.UTF_8))).intValue();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return -1;
        }
    }
}
