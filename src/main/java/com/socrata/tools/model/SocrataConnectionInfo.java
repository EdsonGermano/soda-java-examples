package com.socrata.tools.model;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *  This class contains all the information needed to connect to a
 *  Socrata site.
 *
 *  This is normally loaded from the configuration file that contains all
 *  the connection information.
 */
public class SocrataConnectionInfo
{
    public final String url;
    public final String user;
    public final String password;
    public final String token;

    @JsonCreator
    public SocrataConnectionInfo(@JsonProperty(value="url") String url,
                                 @JsonProperty(value="user") String user,
                                 @JsonProperty(value="password") String password,
                                 @JsonProperty(value="token") String token)
    {
        this.url = url;
        this.user = user;
        this.password = password;
        this.token = token;
    }

    public String getUrl()
    {
        return url;
    }

    public String getUser()
    {
        return user;
    }

    public String getPassword()
    {
        return password;
    }

    public String getToken()
    {
        return token;
    }
}
