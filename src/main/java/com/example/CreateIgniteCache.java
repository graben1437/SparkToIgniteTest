package com.example;


import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.ArrayList;
import java.util.List;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

public class CreateIgniteCache {

    private static final String TEST_CACHE_NAME = "TESTCACHE";

    public static void main(String args[]) {
        Ignite ignite = null;
        try {
            CreateIgniteCache me = new CreateIgniteCache();
            ignite = me.connectToIgnite();
            System.out.println("CreateIgniteCache: Connected To Grid.");

            CacheConfiguration<Integer, SampleTable> sampleIgniteCacheCfg = new CacheConfiguration<>(TEST_CACHE_NAME);

            sampleIgniteCacheCfg.setCacheMode(CacheMode.PARTITIONED);
            // statusCacheCfg.setCacheMode(CacheMode.REPLICATED);
            sampleIgniteCacheCfg.setIndexedTypes(String.class, SampleTable.class);

            // sampleIgniteCacheCfg.setAtomicityMode(TRANSACTIONAL);
            sampleIgniteCacheCfg.setAtomicityMode(ATOMIC);

            IgniteCache<Integer, SampleTable> sampleIgniteCache = ignite.getOrCreateCache(sampleIgniteCacheCfg);
            System.out.println("CreateIgniteCache: sampleIgniteCache created or reinitialized.");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (ignite != null)
                ignite.close();
        }


    }

    private Ignite connectToIgnite() {
        Ignite ignite = null;
        try {
            TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
            TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
            List<String> addressList = new ArrayList<>();
            // turbo1
            // TODO: hard coded IP address and port of Ignite nodes

            // this is the "turbo" cluster - all 4 nodes
            addressList.add("10.100.80.106:47500");
            addressList.add("10.100.80.107:47500");
            addressList.add("10.100.80.108:47500");
            addressList.add("10.100.80.109:47500");

            ipFinder.setAddresses(addressList);
            tcpDiscoverySpi.setIpFinder(ipFinder);
            // turbo4
            // TODO: hard coded IP address and port of Ignite nodes
            tcpDiscoverySpi.setLocalAddress("10.100.80.109");

            IgniteConfiguration cfg = new IgniteConfiguration();
            cfg.setClientMode(true);

            cfg.setDiscoverySpi(tcpDiscoverySpi);

            ignite = Ignition.start(cfg);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return ignite;
    }
}
