# SDC_ETL_LIBS

This package contains many libraries that we use here at SDC for ETL purposes.


##AWS Helpers
This is a helper class that provides common functionality for various aws services. Getting files from s3, inserting into dynamodb etc...

##SDC Dataframes
This a wrapper class for different types of dataframes. The module helps with loading data and writing data to snowflake.
####Supported dataframes
* Pandas

##API Helpers
Contains a factory to help with getting data from various Api endpoints.
####Supported api's
* ExactTarget
* Podium
* New Relic
* TimeControl
* Ultipro
##Database Helpers
Contains a factory to help make connections to several database types
####Supported Types
* Snowflake
* Mysql
* Nexus
