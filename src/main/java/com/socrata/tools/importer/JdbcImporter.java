package com.socrata.tools.importer;

import au.com.bytecode.opencsv.CSVWriter;
import com.socrata.api.HttpLowLevel;
import com.socrata.api.Soda2Producer;
import com.socrata.api.SodaImporter;
import com.socrata.builders.DatasetBuilder;
import com.socrata.exceptions.SodaError;
import com.socrata.tools.model.DataImportConfiguration;
import com.socrata.tools.model.ImportConfiguration;
import com.socrata.tools.model.JdbcConnectionInfo;
import com.socrata.tools.model.SocrataConnectionInfo;
import com.socrata.model.UpsertResult;
import com.socrata.model.importer.Column;
import com.socrata.model.importer.Dataset;
import com.socrata.model.importer.DatasetInfo;
import com.socrata.tools.utils.ConfigurationLoader;
import com.socrata.utils.ColumnUtil;
import org.codehaus.jackson.map.ObjectMapper;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a class that is able to import a query on a database into a dataset in Socrata.
 * It's primary purpose is to provide an example for how to create datasets and import data
 * into them.
 *
 * In this file, there are two main ways import data into Socrata:
 * <ul>
 *     <li>For a small set of results, the most efficient way to import is to directly import
 *     objects using the upsert functionality.  See updateDatasetFromQuery and createDatasetFromQuery.</li>
 *     <li>For larger set of results, the best way to update is to go through the publishing workflow
 *     and import as a CSV.  See updateDatasetFromBigQuery and createDatasetFromBigQuery.</li>
 * </ul>
 *
 *
 * NOTE:  THis class is not thread safe, because is stores a JDBC connection as
 * a member variable.
 */
@NotThreadSafe
public class JdbcImporter
{
    public static final File DEFAULT_CONFIG = new File("import_config.json");
    public static final ObjectMapper objectMapper = new ObjectMapper();

    private Connection  jdbcConnection;

    final JdbcConnectionInfo    jdbcConnectionInfo;
    final SodaImporter          sodaImporter;
    final Soda2Producer         soda2Producer;


    /**
     * This main function allows this import to be run from the commandline.
     *
     * This will take a single argument for now, which is the configuration file
     * to run from.
     *
     * @param arg list of argumentes
     */
    public static void main(String arg[]) throws ClassNotFoundException, SQLException, InterruptedException, SodaError, IOException
    {

        File    configFile = DEFAULT_CONFIG;
        if (arg.length > 0) {
            configFile = new File(arg[0]);
        }

        final ImportConfiguration importConfiguration = ConfigurationLoader.loadConfig(configFile);
        JdbcImporter    jdbcImporter = new JdbcImporter(importConfiguration.getSocrataConnectionInfo(), importConfiguration.getJdbcConnectionInfo());

        for (Map.Entry<String, DataImportConfiguration> entry : importConfiguration.getDatasetsToImport().entrySet()) {

            DataImportConfiguration dataImportConfiguration = entry.getValue();
            System.out.println("Importing: " + entry.getKey() + ".  With query=\"" + dataImportConfiguration.importQuery + "\"");

            DatasetInfo createdDataset = jdbcImporter.createDatasetFromBigQuery(entry.getKey(), dataImportConfiguration.description, dataImportConfiguration.importQuery);
            System.out.println("  Successfully created " + createdDataset.getId());
        }
    }

    /**
     * Creates a JDBC Importer with all the connection information needed for connecting to the
     * database as well as Socrata.
     *
     * @param socrataConnectionInfo
     * @param jdbcConnectionInfo
     */
    public JdbcImporter(SocrataConnectionInfo socrataConnectionInfo, JdbcConnectionInfo jdbcConnectionInfo)
    {
        final HttpLowLevel    httpLowLevel = HttpLowLevel.instantiateBasic(socrataConnectionInfo.getUrl(),
                                                                           socrataConnectionInfo.getUser(),
                                                                           socrataConnectionInfo.getPassword(),
                                                                           socrataConnectionInfo.getToken());
        this.soda2Producer = new Soda2Producer(httpLowLevel);
        this.sodaImporter = new SodaImporter(httpLowLevel);
        this.jdbcConnectionInfo = jdbcConnectionInfo;
    }


    /**
     * Will issue a query and then create a dataset and import the
     * results based on it.
     *
     * @param name the name of the dataset to create
     * @param description the description of the dataset to create
     * @param query the query to issue to export data from the database.
     * @return the results of the upsert
     */
    public DatasetInfo createDatasetFromQuery(String name, String description, String query) throws SQLException, ClassNotFoundException, SodaError, InterruptedException
    {
        //  Execute the JDBC Query
        final ResultSet resultSet = executeQuery(query);

        //  Create a Socrata Dataset from the resultset
        final DatasetInfo dataset = createDataset(name, description, resultSet);

        //  Now, add the results from the query into the dataset
        final UpsertResult retVal = upsertQueryResults(dataset, resultSet);

        //  Finally, publish the changes on the dataset
        sodaImporter.publish(dataset.getId());
        return dataset;
    }


    /**
     * Will issue a query, and update an existing dataset from it using upserts.
     *
     * @param dataset the dataset to update
     * @param query the query to issue to find teh rows to update from
     * @return The upsert result
     */
    public UpsertResult updateDatasetFromQuery(final Dataset dataset, final String query) throws SQLException, ClassNotFoundException, SodaError, InterruptedException
    {
        //  Execute the JDBC Query
        final ResultSet resultSet = executeQuery(query);

        //  Now, add the results from the query into the dataset
        return upsertQueryResults(dataset, resultSet);
    }

    /**
     * Create a dataset from a query.  This method is best called for queries that will return more
     * than 10,000 results.
     *
     * @param name name of the dataset to create
     * @param description description of the dataset to create
     * @param query query to issue
     * @return the created dataset.
     */
    public DatasetInfo createDatasetFromBigQuery(String name, String description, String query) throws SQLException, ClassNotFoundException, SodaError, InterruptedException, IOException
    {
        //  Execute the JDBC Query
        final ResultSet resultSet = executeQuery(query);

        //  Create a Socrata Dataset from the resultset
        final DatasetInfo dataset = createDataset(name, description, resultSet);

        //  Now, add the results from the query into the dataset
        updateDatasetFromBigQuery(dataset, resultSet);

        return dataset;
    }

    /**
     * Appends the results of a query into a dataset.  This will use the publish cycle and
     * CSV import to append the results.  This method is best called for queries that will return more
     * than 10,000 results.
     *
     * @param dataset dataset to add rows to
     * @param query query to pull results from.
     */
    public void updateDatasetFromBigQuery(final Dataset dataset, final String query) throws SQLException, ClassNotFoundException, IOException, SodaError, InterruptedException
    {
        //  Execute the JDBC Query
        final ResultSet resultSet = executeQuery(query);

        //Update the dataset
        updateDatasetFromBigQuery(dataset, resultSet, false);
    }

    /**
     * Appends the results in a resultset into a dataset.  This will use the publish cycle and
     * CSV import to append the results.  This method is best called for queries that will return more
     * than 10,000 results.
     *
     * @param dataset dataset to add rows to
     * @param resultSet query to pull results from.
     */
    public void updateDatasetFromBigQuery(final DatasetInfo dataset, final ResultSet resultSet) throws SQLException, ClassNotFoundException, IOException, SodaError, InterruptedException
    {
        updateDatasetFromBigQuery(dataset, resultSet, true);
    }


    /**
     * Appends the results in a resultset into a dataset.  This will use the publish cycle and
     * CSV import to append the results.  This method is best called for queries that will return more
     * than 10,000 results.
     *
     * @param dataset dataset to add rows to
     * @param resultSet query to pull results from.
     * @param createWorkingCopy whether to create a working copy or not.  If this is false, the dataset should
     *                          already be a working copy, but will be published as part of this call.
     */
    public void updateDatasetFromBigQuery(final DatasetInfo dataset, final ResultSet resultSet, boolean createWorkingCopy) throws SQLException, ClassNotFoundException, IOException, SodaError, InterruptedException
    {

        //  Write as a csv file
        final File tempFile = writeResultsAsFile(resultSet);

        try {
            //Create a working copy, then append the results
            final DatasetInfo workingCopy = createWorkingCopy ? sodaImporter.createWorkingCopy(dataset.getId()) : dataset;
            sodaImporter.append(workingCopy.getId(), tempFile, 1, null);
            sodaImporter.publish(workingCopy.getId());

        } finally {
            tempFile.delete();
        }

    }

    /**
     * Writes out the resultset as a GZipped CSV file.
     *
     * @param resultSet
     * @return
     * @throws IOException
     * @throws SQLException
     */
    protected File writeResultsAsFile(final ResultSet resultSet) throws IOException, SQLException
    {
        final File retVal = File.createTempFile("SocrataImport", ".csv");
        retVal.deleteOnExit();

        final FileOutputStream      fos = new FileOutputStream(retVal);
        final OutputStreamWriter    writer = new OutputStreamWriter(fos);
        final CSVWriter csvWriter = new CSVWriter(writer);

        //Make sure column names are escaped properly, so they match up with the
        //created dataset
        String[] columnHeaders = new String[resultSet.getMetaData().getColumnCount()];
        for (int i=1; i<resultSet.getMetaData().getColumnCount(); i++) {
            columnHeaders[i-1] = ColumnUtil.getQueryName(resultSet.getMetaData().getColumnName(i));
        }
        csvWriter.writeNext(columnHeaders);
        csvWriter.writeAll(resultSet, false);
        writer.close();

        return retVal;
    }


    /**
     * Appends the results of a query to a dataset.
     *
     * @param dataset
     * @param resultSet
     * @return
     */
    public UpsertResult upsertQueryResults(DatasetInfo dataset, ResultSet resultSet) throws SodaError, InterruptedException, SQLException
    {
        List results = new ArrayList();
        while (resultSet.next()) {
            results.add(convertRowToObject(resultSet));
        }
        return soda2Producer.upsert(dataset.getId(), results);
    }

    /**
     * Creates a Socrata Dataset from a ResultSet Metadata.  This will
     * look at the metadata for the resultset to figure out the names and types
     * of the columns for creating.
     *
     * @param name name of the dataset to create
     * @param description the description of the dataset to create
     * @param resultSet the resultset that comes as a result of the last query.
     * @return The dataset that was created.
     */
    public DatasetInfo createDataset(String name, String description, ResultSet resultSet) throws SQLException, SodaError, InterruptedException
    {
        final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        //Create a dataset with the appropriate name + description
        DatasetBuilder builder = new DatasetBuilder();
        builder.setName(name)
                .setDescription(description);

        //Add the columns to the dataset
        final int numColumns = resultSetMetaData.getColumnCount();
        for (int i=1; i<=numColumns; i++) {

            final String columnName = resultSetMetaData.getColumnName(i);
            String soqlType = convertToSoqlType(resultSetMetaData.getColumnType(i));
            builder.addColumn(new Column(null, columnName, ColumnUtil.getQueryName(columnName), columnName, soqlType, i-1, 200));
        }

        //Create the dataset on the Socrata side
        return sodaImporter.createDataset(builder.build());

    }

    /**
     * Converts a row being returned from the JDBC Resultset into an
     * Object that can be written out as a JSON object.
     *
     * @param resultSet the JDBC resultset that has already had "next" called on it
     * @return a Map from the column names to their Java values.
     * @throws SQLException
     */
    public Map<String, Object> convertRowToObject(ResultSet resultSet) throws SQLException
    {

        final Map<String, Object> retVal = new HashMap<String, Object>();
        final ResultSetMetaData metaData = resultSet.getMetaData();

        for (int i=1; i<=metaData.getColumnCount(); i++) {
            retVal.put(ColumnUtil.getQueryName(metaData.getColumnName(i)), resultSet.getObject(i));
        }
        return retVal;
    }

    /**
     * Converts from a SQL type to the Socrata types.
     *
     * @param sqlType a SqlType returned from the JDBC Resultset.
     * @return the Socrata Data Type to use
     */
    static protected String convertToSoqlType(int sqlType) {
        switch (sqlType) {
            case Types.BIGINT:
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.INTEGER:
            case Types.NUMERIC:
            case Types.REAL:
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.ROWID:
                return "Number";

            case Types.BIT:
            case Types.BOOLEAN:
                return "checkbox";

            case Types.BLOB:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                return "document";

            case Types.CHAR:
            case Types.CLOB:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.VARCHAR:
                return "text";

            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return "calendar_date";

            default:
                throw new IllegalArgumentException("Type " + sqlType + " is not supported.");

        }
    }

    protected Connection assureConnection() throws SQLException, ClassNotFoundException
    {
        if (jdbcConnection == null) {
            Class.forName(jdbcConnectionInfo.driverClass);
            jdbcConnection = DriverManager.getConnection(jdbcConnectionInfo.connectionString, jdbcConnectionInfo.userName, jdbcConnectionInfo.password);
        }
        return jdbcConnection;
    }


    protected void assureConnectionClosed() throws SQLException, ClassNotFoundException
    {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
    }

    protected ResultSet executeQuery(final String query) throws SQLException, ClassNotFoundException
    {
        final Connection  connection  = assureConnection();
        final Statement   stmt        = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        return stmt.executeQuery(query);
    }

}
