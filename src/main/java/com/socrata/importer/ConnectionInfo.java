package com.socrata.importer;

/**
 *
 */
public class ConnectionInfo
{
    public final String driverClass;
    public final String connectionString;
    public final String userName;
    public final String password;

    public ConnectionInfo(String driverClass, String connectionString, String userName, String password)
    {
        this.driverClass = driverClass;
        this.connectionString = connectionString;
        this.userName = userName;
        this.password = password;
    }
}
