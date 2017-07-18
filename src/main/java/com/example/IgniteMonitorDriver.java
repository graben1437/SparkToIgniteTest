package com.example;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Generates some statistics on the Ignite cluster for all caches
 * and a hard coded table
 *
 * Created by david.robinson on 7/13/17.
 */
public class IgniteMonitorDriver {

    public static void main(String[] args) {
        IgniteMonitorDriver imd = new IgniteMonitorDriver();
        imd.doWork();
    }

    private void doWork() {
        Ignite ignite = null;

        try {
            // connect to ignite and list the separate caches (like db schemas)
            ignite = connectToIgnite2();
            Collection<String> cacheNames = ignite.cacheNames();
            for (String aCache: cacheNames) {
                System.out.println("cache name: " + aCache);
            }

            // now list the "SQL table names" for each cache
            // why don't they match ?
            // this fails and I posted my results on stack overflow
            // https://stackoverflow.com/questions/43049782/apache-ignite-how-to-list-all-tables-and-all-caches/43641694#43641694
            /* for (String aCache: cacheNames) {
                IgniteCache<String, ?> aCacheHandle = ignite.cache(aCache);
                aCacheHandle.
                for (String aTable: tableNames) {
                    if (aTable.contains("SAMPLETABLE")) {
                        IgniteCache<Integer, SampleTable> table = ignite.cache(aTable);
                        CacheMetrics theMetrics = table.metrics();
                        System.out.println("table metrics - size: " + theMetrics.getSize());
                        System.out.println("table metrics - off heap entries: " + theMetrics.getOffHeapEntriesCount() );
                        CacheConfiguration ccfg = table.getConfiguration(CacheConfiguration.class);
                        Collection<QueryEntity> entities = ccfg.getQueryEntities();
                        for (QueryEntity entity : entities) {
                            System.out.println("SAMPLETABLE entity: " + entity.getTableName());
                        }
                    }
                }
            } */

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            ignite.close();
        }
    }



    private Ignite connectToIgnite2() {
        Ignite ignite = null;
        try {
            TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
            TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
            List<String> addressList = new ArrayList<>();
            // turbo1
            // TODO: hard coded IP address and port of Ignite nodes
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
