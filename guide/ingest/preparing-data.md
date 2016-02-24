# Preparing data
Data may be obtained from multiple sources, such as transaction logs, database snapshots and segmentation models.

##Facts

Data must be tranformed into _facts_ before it can be ingested and stored in Ivory.

A fact has these components:

&lt;Entity&gt; &lt;Attribute&gt; &lt;Value&gt; &lt;Time&gt;

That is, a fact represents the _value_ of an _attribute_ associated with an _entity_, which is known to be valid at some point in _time_.

Examples of facts are:

| Entity | Attribute | Value | Time |
|:------------ |:------------ |:------------ |:------------ |
| cust_00678 | gender | M | 2011-03-17 |
| acnt_1234 | balance | 342.17 | 2014-06-01 |
| car_98732 | make | Toyota | 2012-09-25 |


The attributes referenced in facts must be declared in the Ivory repository's dictionary. The dictionary lists all known attributes and the metadata associated with them.

##Transforming data

You can use various tools to transform data into a format that Ivory can ingest:

* SQL
* Python scripts
* [Apache Thrift](https://thrift.apache.org/)

Ivory can ingest facts in these formats:

* Text - human readable, and can include JSON structs, but is not recommended for production due to performance reasons.

* Binary - generated using Thrift, for example, and recommended for production.
