package com.socrata.tools;

import com.google.common.collect.Lists;
import com.socrata.api.HttpLowLevel;
import com.socrata.api.Soda2Consumer;
import com.socrata.api.Soda2Producer;
import com.socrata.api.SodaDdl;
import com.socrata.builders.SoqlQueryBuilder;
import com.socrata.exceptions.LongRunningQueryException;
import com.socrata.exceptions.SodaError;
import com.socrata.model.UpsertResult;
import com.socrata.model.importer.Column;
import com.socrata.model.importer.Dataset;
import com.socrata.model.importer.DatasetInfo;
import com.socrata.model.soql.OrderByClause;
import com.socrata.model.soql.SoqlQuery;
import com.socrata.model.soql.SortOrder;
import com.socrata.tools.utils.ConfigurationLoader;
import com.socrata.tools.model.SocrataConnectionInfo;
import com.socrata.tools.utils.CliUtils;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 */
public class CopyDataset
{

   public static final Option DEST_DOMAIN   = OptionBuilder.withArgName("destUrl" )
                                         .hasArg()
                                         .withDescription(  "The url for the destination domain, e.g. https://foo.bar.baz .  This defaults to being the same domain as the source domain" )
                                         .create("d");

    public static final Option SOURCE_DOMAIN   = OptionBuilder.withArgName("srcDomain")
                                          .hasArg()
                                          .withDescription(  "The url for the source domain.  This defaults to what is loaded in the connection config." )
                                          .create("s");

    public static final Option CONFIG_FILE   = OptionBuilder.withArgName("connectionConfig")
                                          .hasArg()
                                          .withDescription(  "The configuration file to load for user source domain URL/name/password/apptoken.  Defaults to ~/.socrata/connection.json" )
                                          .create("c");

    public static final Option DATA_FILE   = OptionBuilder.withArgName("dataFile")
                                           .hasArg()
                                           .withDescription("The directory to look for data files to upload to the newly created dataset.  " +
                                                                    "The tool will look for files that have the same name as the dataset id they go with.  " +
                                                                    "These can be json or csv.  In addition, the can be gzipped.")
                                           .create("f");

    public static final Option CREATE_ONLY   = OptionBuilder.withArgName("createOnly")
                                           .withDescription("Don't copy over data, ONLY create the new dataset.")
                                           .create("C");

    public static final Option COPY_DATA   = OptionBuilder.withArgName("copyDataLive")
                                                            .withDescription("Do NOT use a file to import data.  Copy it directly from the live dataset.")
                                                            .create("p");


    public static final Option CREATE_OPTIONS   = OptionBuilder.withArgName("createOptions")
                                            .hasArg()
                                            .withDescription("This adds an option that should be passed on the URL when creating the dataset.  E.g. $$testflag=true .")
                                            .create("o");

    public static final Option USAGE_OPTIONS   = OptionBuilder.withArgName("?")
                                                               .withDescription("Shows usage.")
                                                               .create("?");


    public static final Options OPTIONS = new Options();

    static {
        OPTIONS.addOption(DEST_DOMAIN);
        OPTIONS.addOption(SOURCE_DOMAIN);
        OPTIONS.addOption(CONFIG_FILE);
        OPTIONS.addOption(DATA_FILE);
        OPTIONS.addOption(CREATE_ONLY);
        OPTIONS.addOption(CREATE_OPTIONS);
        OPTIONS.addOption(COPY_DATA);
        OPTIONS.addOption(USAGE_OPTIONS);
    }


    final boolean createOnly;
    final boolean copyDataLive;
    final String srcDomain;
    final String destDomain;
    final String userName;
    final String userPassword;
    final String token;
    final File   dataFileDir;
    final List<Pair<String, String>> parsedCreateOptions;

    /**
     * DatasetId
     * userName
     * userPassword
     * token
     * @param args
     */
    public static void main(String[] args) throws SodaError, InterruptedException, IOException, LongRunningQueryException
    {

        CommandLineParser   parser = new PosixParser();


        try {
            CommandLine         cmd = parser.parse(OPTIONS, args, false);
            String configFile = cmd.getOptionValue("c", CliUtils.defaultConfigFile().getCanonicalPath());

            if (cmd.hasOption("?")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "copydataset", OPTIONS );
                System.exit(1);
            }

            try {

                final File config = new File(configFile);
                if (!config.canRead()) {
                    throw new IllegalArgumentException("Unable to load connection configuration from " + configFile + ".  Either use the -c option or setup a connection file there.");
                }

                final SocrataConnectionInfo connectionInfo = ConfigurationLoader.loadSocrataConnectionConfig(config);
                CliUtils.validateConfiguration(connectionInfo);

                final String srcDomain = cmd.getOptionValue("s", connectionInfo.getUrl());
                if (StringUtils.isEmpty(srcDomain)) {
                    throw new IllegalArgumentException("No source domain specified in either the connection configuration or the commandline arguments.");
                }

                final String destDomain = cmd.getOptionValue("d", srcDomain);
                final String createOptions = cmd.getOptionValue("o");
                final List<Pair<String, String>> parsedCreateOptions = CliUtils.parseOptions(createOptions, Charset.defaultCharset());
                final File dataFileDir = new File(cmd.getOptionValue("f", "."));
                final boolean copyDataLive = cmd.hasOption("p");
                final boolean createOnly = cmd.hasOption("C");


                final CopyDataset copyDataset = new CopyDataset(srcDomain, destDomain, connectionInfo.getUser(), connectionInfo.getPassword(), connectionInfo.getToken(), dataFileDir, parsedCreateOptions, createOnly, copyDataLive);
                final List<Pair<Dataset, UpsertResult>> results = copyDataset.doCopy(cmd.getArgs());

                for (Pair<Dataset, UpsertResult> result : results) {
                    System.out.println("Created dataset " + destDomain + "/id/" + result.getKey().getId() + ".  Created " + result.getValue().getRowsCreated());
                }

            } catch (IllegalArgumentException e) {
                System.out.println(e.getMessage());
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "copydataset", OPTIONS );
                System.exit(1);
            }
        } catch (ParseException e) {
            System.err.println( "Parsing failed.  Reason: " + e.getMessage() );

            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "copydataset", OPTIONS );
            System.exit(1);
        }
    }

    public CopyDataset(String srcDomain, String destDomain, String userName, String userPassword, String token, File dataFileDir, List<Pair<String, String>> parsedCreateOptions, boolean createOnly, boolean copyDataLive)
    {
        this.srcDomain = srcDomain;
        this.destDomain = destDomain;
        this.userName = userName;
        this.userPassword = userPassword;
        this.token = token;
        this.dataFileDir = dataFileDir;
        this.parsedCreateOptions = parsedCreateOptions;
        this.createOnly = createOnly;
        this.copyDataLive = copyDataLive;
    }

    public List<Pair<Dataset, UpsertResult>> doCopy(String[] datasetIds) throws SodaError, InterruptedException, LongRunningQueryException, IOException
    {
        List<Pair<Dataset, UpsertResult>>   results = Lists.newArrayList();
        for (String datasetId : datasetIds) {
            results.add(doCopy(datasetId));
        }
        return results;
    }

    public Pair<Dataset, UpsertResult> doCopy(String datasetId) throws SodaError, InterruptedException, LongRunningQueryException, IOException
    {

        final SodaDdl ddlSrc = SodaDdl.newDdl(srcDomain, userName, userPassword, token);
        final SodaDdl ddlDest = SodaDdl.newDdl(destDomain, userName, userPassword, token);


        for (Pair<String, String> createOption : parsedCreateOptions) {
            ddlDest.getHttpLowLevel().getAdditionalParameters().put(createOption.getKey(), createOption.getValue());
        }

        final Dataset srcDataset = loadSourceSchema(ddlSrc, datasetId);
        final DatasetInfo destDatasetTemplate = Dataset.copy(srcDataset);
        final Dataset destDataset = createDestSchema(ddlDest, srcDataset, destDatasetTemplate);
        ddlDest.publish(destDataset.getId());

        final Soda2Producer producerDest = Soda2Producer.newProducer(destDomain, userName, userPassword, token);

        UpsertResult    upsertResult = new UpsertResult(0, 0, 0, null);

        //Now for the data part
        if (!createOnly) {
            if (copyDataLive) {
                upsertResult = copyDataLive(producerDest, srcDataset.getId(), destDataset.getId());
            } else {
                upsertResult = importDataFile(producerDest, destDataset.getId(), dataFileDir);
            }
        }

        return Pair.of(destDataset, upsertResult);
    }


    public static Dataset loadSourceSchema(SodaDdl ddlSrc, String datasetId) throws SodaError, InterruptedException
    {
        final DatasetInfo datasetInfo = ddlSrc.loadDatasetInfo(datasetId);
        if (!(datasetInfo instanceof Dataset)) {
            throw new SodaError("Can currently only copy datasets.");
        }

        return (Dataset) datasetInfo;
    }

    public static Dataset createDestSchema(SodaDdl ddlDest, Dataset srcDataset, DatasetInfo destDatasetTemplate) throws SodaError, InterruptedException
    {
        Dataset newDataset = (Dataset) ddlDest.createDataset(destDatasetTemplate);
        for (Column column  : srcDataset.getColumns()) {
            ddlDest.addColumn(newDataset.getId(), column);
        }

        return (Dataset) ddlDest.loadDatasetInfo(newDataset.getId());
    }

    @Nonnull
    public static UpsertResult importDataFile(Soda2Producer producerDest, String destId, File dataFile) throws IOException, SodaError, InterruptedException
    {
        if (!dataFile.exists()) {
            throw new SodaError(dataFile.getCanonicalPath() + " does not exist.");
        }

        String unprocecessedName = dataFile.getName();
        String extToProcess = getExtension(unprocecessedName);
        InputStream is = new FileInputStream(dataFile);

        if (extToProcess.equals(".gz")) {
            is = new GZIPInputStream(is);
            unprocecessedName = unprocecessedName.substring(0, unprocecessedName.length() - extToProcess.length());
            extToProcess = getExtension(unprocecessedName);
        }

        MediaType mediaType = HttpLowLevel.CSV_TYPE;
        if (extToProcess.equals(".json")) {
            mediaType = HttpLowLevel.JSON_TYPE;
        }

        return producerDest.upsertStream(destId, mediaType, is);
    }

    public UpsertResult copyDataLive(Soda2Producer producerDest, String srcId, String destId) throws LongRunningQueryException, SodaError, InterruptedException
    {

        final Soda2Consumer querySource = Soda2Consumer.newConsumer(srcDomain, userName, userPassword, token);

        SoqlQueryBuilder    builder = new SoqlQueryBuilder(SoqlQuery.SELECT_ALL)
                .addOrderByPhrase(new OrderByClause(SortOrder.Ascending, ":id"))
                .setLimit(1000);

        int         offset = 0;
        long        rowsAdded = 0;
        boolean     hasMore = true;

        while (hasMore) {
            ClientResponse response = querySource.query(srcId, HttpLowLevel.JSON_TYPE, builder.setOffset(offset).build());
            UpsertResult result = producerDest.upsertStream(destId, HttpLowLevel.JSON_TYPE, response.getEntityInputStream());

            offset+=1000;
            rowsAdded+=result.getRowsCreated();

            if (result.getRowsCreated() == 0) {
                hasMore = false;
            }
        }
        return new UpsertResult(rowsAdded, 0, 0, null);
    }

    @Nonnull
    private static String getExtension(String fileName) {
        final int lastDot = fileName.lastIndexOf('.');
        if (lastDot == -1 || lastDot==fileName.length()) {
            return "";
        }

        return fileName.substring(lastDot);
    }
}
