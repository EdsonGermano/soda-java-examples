package com.socrata.tools.importer;

import com.socrata.exceptions.SodaError;
import com.socrata.tools.model.JdbcConnectionInfo;
import com.socrata.tools.model.SocrataConnectionInfo;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.sql.SQLException;

/**
 * This is a test only importer that works if a site does not have a valid certificate yet.
 */
public class InsecureJdbcImport extends JdbcImporter
{
    static {
        // Create a trust manager that does not validate certificate chains.  This is only
        //needed when running tests against a non-official SODA2 instance.
        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager(){
            public X509Certificate[] getAcceptedIssuers(){return null;}
            public void checkClientTrusted(X509Certificate[] certs, String authType){}
            public void checkServerTrusted(X509Certificate[] certs, String authType){}
        }};

        // Install the all-trusting trust manager
        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, trustAllCerts, new SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (Exception e) {
            ;
        }
    }

    public static void main(String arg[]) throws ClassNotFoundException, SQLException, InterruptedException, SodaError, IOException
    {
        JdbcImporter.main(arg);
    }

    public InsecureJdbcImport(SocrataConnectionInfo socrataConnectionInfo, JdbcConnectionInfo jdbcConnectionInfo)
    {
        super(socrataConnectionInfo, jdbcConnectionInfo);
    }
}
