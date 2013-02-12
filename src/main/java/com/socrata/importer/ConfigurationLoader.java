package com.socrata.importer;

import com.socrata.importer.model.ImportConfiguration;
import com.socrata.importer.model.SocrataConnectionInfo;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;

/**
 */
public class ConfigurationLoader
{
    static public final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static public ImportConfiguration loadConfig(final File file)
    {
        try {
            return OBJECT_MAPPER.readValue(file, ImportConfiguration.class);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    static public SocrataConnectionInfo loadSocrataConfig(final File file)
    {
        try {
            return OBJECT_MAPPER.readValue(file, SocrataConnectionInfo.class);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
