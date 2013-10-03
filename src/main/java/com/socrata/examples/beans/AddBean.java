package com.socrata.examples.beans;

import com.google.common.collect.Lists;
import com.socrata.api.HttpLowLevel;
import com.socrata.api.Soda2Producer;
import com.socrata.api.SodaImporter;
import com.socrata.builders.DatasetBuilder;
import com.socrata.exceptions.SodaError;
import com.socrata.tools.utils.ConfigurationLoader;
import com.socrata.tools.model.SocrataConnectionInfo;
import com.socrata.model.UpsertResult;
import com.socrata.model.importer.Column;
import com.socrata.model.importer.Dataset;
import com.socrata.model.importer.DatasetInfo;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

/**
 * This class simply provides an example of adding objects to
 * Socrata using standard Java Beans.
 */
public class AddBean
{
    public static final File DEFAULT_CONFIG = new File("socrata_config.json");

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

        //Load config
        final SocrataConnectionInfo importConfiguration = ConfigurationLoader.loadSocrataConnectionConfig(configFile);
        final AddBean     addBean = new AddBean(importConfiguration);

        //Create the dataset
        DatasetInfo datasets = addBean.createDataset("TestDataset", "TestDataset Description");

        //Create a few Beans.  In a real program, this would probably come from Hibernate or some other
        //ORM used to pull results out of a databse.
        List<TestBean>   updateList = Lists.newArrayList(
                new TestBean("first", "1", 2, new Date()),
                new TestBean("second", "2", 3, new Date()),
                new TestBean("third", "3", 4, new Date())
        );

        addBean.addBeansToDataset(datasets, updateList);
    }

    /**
     * Creates a JDBC Importer with all the connection information needed for connecting to the
     * database as well as Socrata.
     *
     * @param socrataConnectionInfo
     */
    public AddBean(SocrataConnectionInfo socrataConnectionInfo)
    {
        final HttpLowLevel httpLowLevel = HttpLowLevel.instantiateBasic(socrataConnectionInfo.getUrl(),
                                                                        socrataConnectionInfo.getUser(),
                                                                        socrataConnectionInfo.getPassword(),
                                                                        socrataConnectionInfo.getToken());
        this.soda2Producer = new Soda2Producer(httpLowLevel);
        this.sodaImporter = new SodaImporter(httpLowLevel);
    }

    /**
     * Creates a dataset to add beans to.
     *
     *
     * @param name name of the dataset to add
     * @param description description of the dataset
     * @return the added dataset
     */
    public DatasetInfo createDataset(String name, String description) throws SodaError, InterruptedException
    {
        DatasetBuilder  datasetBuilder = new DatasetBuilder()
                .setName(name)
                .setDescription(description)
                .addColumn(new Column(null, "name", "name", "name Description", "text", 1, 120))
                .addColumn(new Column(null, "value", "value", "value Description", "text", 2, 120))
                .addColumn(new Column(null, "count", "count", "count Description", "Number", 3, 120))
                .addColumn(new Column(null, "date", "date", "date Description", "calendar_date", 4, 120));

        Dataset dataset = (Dataset) sodaImporter.createDataset(datasetBuilder.build());
        dataset.setupRowIdentifierColumnByName("name");
        sodaImporter.updateDatasetInfo(dataset);
        return sodaImporter.publish(dataset.getId());
    }

    /**
     * Takes some beans and adds them to the dataset
     *
     * @param dataset to add to
     * @param beansToAdd list of beans to add
     * @return results from the add
     */
    public UpsertResult addBeansToDataset(DatasetInfo dataset, List<TestBean> beansToAdd) throws SodaError, InterruptedException
    {
        return soda2Producer.upsert(dataset.getId(), beansToAdd);
    }



}
