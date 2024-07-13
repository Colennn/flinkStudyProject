package com.ym.gmall.common.mock;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class InternetUtil {

    public static void main(String[] args) {
        getLocalHost();
    }

    public static String getLocalHost() {
        Enumeration<NetworkInterface> nifs = null;
        try {
            nifs = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        while (nifs.hasMoreElements()) {
            NetworkInterface nif = nifs.nextElement();
            // 检查网络接口名称是否以 "en0" 开头
            if (nif.getName().startsWith("en0")) {
                // 获得与该网络接口绑定的 IP 地址，一般只有一个
                Enumeration<InetAddress> addresses = nif.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    if (addr instanceof Inet4Address) { // 只关心 IPv4 地址
                        System.out.println("en0 网卡接口地址：" + addr.getHostAddress());
                        return addr.getHostAddress();
                    }
                }
            }
        }
        return "";
    }

}