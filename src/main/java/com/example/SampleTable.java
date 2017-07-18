package com.example;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.sql.Timestamp;


/**
 * Cache/Table in Ignite
 */
public class SampleTable {

    // @QuerySqlField
    // private String uniqueid;

    @QuerySqlField
    private int acctnum;

    @QuerySqlField
    private String adminUserName;

    @QuerySqlField
    private String orgName;


    /* public void setUniqueId(String uniqueIdIn) {
        uniqueid = uniqueIdIn;
    }
    public String getUniqueId() {return uniqueid;}
    */

    public void setAcctNum(int acctNumIn) {
        acctnum = acctNumIn;
    }
    public int getAcctnum() {return acctnum;}

    public void setAdminUserName(String adminNameIn) {adminUserName = adminNameIn;}
    public String getAdminUserName() {return adminUserName;}

    public void setOrgName(String orgNameIn) {orgName = orgNameIn;}
    public String getOrgName() {return orgName;}
}
