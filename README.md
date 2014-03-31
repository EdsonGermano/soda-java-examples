soda-java-examples
==================

This has a number of examples using the Socrata libraries and tools showing how to use them.

Currently, the main example is the JdbcImporter.  This shows how to create datasets as well as update results
using both the publishing workflow (using CSV files) and the new publisher flow.

For example, here is a not-necessarily-guaranteed-to-work example of calling InsecureCopyDataset:

java -cp target/soda-api-java-examples-0.5-SNAPSHOT-jar-with-dependencies.jar com.socrata.tools.InsecureCopyDataset -o nbe=true -x ~/.socrata/staging_soda2nbe_config.json -d https://soda2nbe.test-socrata.com/ -c ~/.socrata/production_config.json -s https://data.consumerfinance.gov/ -f ${HOME}/Downloads/rows.csv x94z-ydhh

