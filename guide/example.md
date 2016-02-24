# An Example: Weather Observations

This worked example demonstrates a simple workflow for using Ivory.

Once you have installed Ivory,


## Create an Ivory repo

Create a new Ivory repository:

    $ ivory ???



## Import the dictionary

Import the dictionary directly from S3:

    $ ivory import

Display the dictionary:

    $ ivory ???

The dictionary contains the following:

(show the whole dict file here)

## Transform the data

Transform the data into a format that Ivory can ingest.

You can use your own tool chain, or tools such as Apache Hive or Pig.

For this example, we...

## Ingest the data

A raw data file is available on S3 at s3://ambiata-ivory/example/weather/2010/...

The raw data contains rows like this:

(show a full data row here)

Ingest the raw data directly from S3:

    $ ivory ingest???


## Obtain a snapshot


    $ ivory snapshot ???
