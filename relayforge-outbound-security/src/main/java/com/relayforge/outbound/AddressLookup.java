package com.relayforge.outbound;

import java.net.InetAddress;
import java.net.UnknownHostException;

@FunctionalInterface
public interface AddressLookup {
    InetAddress[] lookup(String host) throws UnknownHostException;
}
