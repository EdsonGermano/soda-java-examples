package com.socrata.importer.model;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This class contains all the information needed to connect to a database
 */
public class JdbcConnectionInfo
{
    public final String driverClass;
    public final String connectionString;
    public final String userName;
    public final String password;

    @JsonCreator
    public JdbcConnectionInfo(@JsonProperty(value="driverClass")        String driverClass,
                              @JsonProperty(value="connectionString")   String connectionString,
                              @JsonProperty(value="userName")           String userName,
                              @JsonProperty(value="password")           String password)
    {
        this.driverClass = driverClass;
        this.connectionString = connectionString;
        this.userName = userName;
        this.password = password;
    }

    public String getDriverClass()
    {
        return driverClass;
    }

    public String getConnectionString()
    {
        return connectionString;
    }

    public String getUserName()
    {
        return userName;
    }

    public String getPassword()
    {
        return password;
    }
}
