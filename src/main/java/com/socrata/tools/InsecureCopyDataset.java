package com.socrata.tools;

import com.socrata.exceptions.LongRunningQueryException;
import com.socrata.exceptions.SodaError;
import org.apache.commons.lang3.tuple.Pair;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * This is a version of copy dataset, that does not do SSL validation.  This is really for test only
 * scenarios, for any real operations just use the CopyDataset class
 */
public class InsecureCopyDataset extends CopyDataset
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

    public InsecureCopyDataset(String srcDomain, String destDomain, String userName, String userPassword, String token, File dataFileDir, List<Pair<String, String>> parsedCreateOptions, boolean createOnly, boolean copyDataLive)
    {
        super(srcDomain, destDomain, userName, userPassword, token, dataFileDir, parsedCreateOptions, createOnly, copyDataLive);
    }

    public static void main(String[] args) throws SodaError, InterruptedException, IOException, LongRunningQueryException
    {
        CopyDataset.main(args);
    }

}
