package com.socrata.tools.utils;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.socrata.tools.model.SocrataConnectionInfo;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

/**
 */
public class CliUtils
{

    static public File defaultConfigFile() {
        File userHome = FileUtils.getUserDirectory();
        return new File(new File(userHome, ".socrata"), "connection.json");
    }

    static public void validateConfiguration(SocrataConnectionInfo connectionInfo) {

        if (StringUtils.isEmpty(connectionInfo.getUser())) {
            throw new IllegalArgumentException("Configuration file does not contain a valid value for 'user'.");
        }

        if (StringUtils.isEmpty(connectionInfo.getPassword())) {
            throw new IllegalArgumentException("Configuration file does not contain a valid value for 'password'.");
        }

        if (StringUtils.isEmpty(connectionInfo.getToken())) {
            throw new IllegalArgumentException("Configuration file does not contain a valid value for 'token'.");
        }

    }

    static public List<Pair<String, String>> parseOptions(String options, Charset charset) {
        if (StringUtils.isEmpty(options)) {
            return Collections.emptyList();
        }

        List<NameValuePair> optionList = URLEncodedUtils.parse(options, charset);
        return Lists.newArrayList(
                Collections2.transform(optionList, new Function<NameValuePair, Pair<String, String>>()
                {
                    public Pair<String, String> apply(NameValuePair input)
                    {
                        return Pair.of(input.getName(), input.getValue());
                    }
                })
        );
    }


}
