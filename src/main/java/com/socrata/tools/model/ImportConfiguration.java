package com.socrata.tools.model;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;

/**
 */
public class ImportConfiguration
{
    final public JdbcConnectionInfo jdbcConnectionInfo;
    final public SocrataConnectionInfo socrataConnectionInfo;
    final public Map<String, DataImportConfiguration>   datasetsToImport;

    @JsonCreator
    public ImportConfiguration(@JsonProperty(value = "jdbcConnectionInfo") JdbcConnectionInfo jdbcConnectionInfo,
                               @JsonProperty(value = "socrataConnectionInfo") SocrataConnectionInfo socrataConnectionInfo,
                               @JsonProperty(value = "datasetsToImport") Map<String, DataImportConfiguration> datasetsToImport)
    {
        this.jdbcConnectionInfo = jdbcConnectionInfo;
        this.socrataConnectionInfo = socrataConnectionInfo;
        this.datasetsToImport = datasetsToImport;
    }

    public JdbcConnectionInfo getJdbcConnectionInfo()
    {
        return jdbcConnectionInfo;
    }

    public SocrataConnectionInfo getSocrataConnectionInfo()
    {
        return socrataConnectionInfo;
    }

    public Map<String, DataImportConfiguration> getDatasetsToImport()
    {
        return datasetsToImport;
    }
}
