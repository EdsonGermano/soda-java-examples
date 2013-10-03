package com.socrata.tools.model;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;

/**
 * This contains a configuration for importing a query to Socrata
 */
public class DataImportConfiguration
{
    final public String description;
    final public Map<String, Map<String, String>> metadata;
    final public String importQuery;

    @JsonCreator
    public DataImportConfiguration(@JsonProperty(value = "description") String description,
                                   @JsonProperty(value = "metadata") Map<String, Map<String, String>> metadata,
                                   @JsonProperty(value = "importQuery") String importQuery)
    {
        this.description = description;
        this.metadata = metadata;
        this.importQuery = importQuery;
    }

    public String getDescription()
    {
        return description;
    }

    public Map<String, Map<String, String>> getMetadata()
    {
        return metadata;
    }
}
