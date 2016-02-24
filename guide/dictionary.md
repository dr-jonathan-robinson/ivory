# The Ivory dictionary
The dictionary lists all known attributes and the metadata associated with them. All attributes referenced in facts must be declared in the Ivory repository's dictionary.

## Entity attributes
An attribute is identified in the dictionary by a namespace and name.  

For example:

<table>
    <tr><td>*Namespace*</td><td>*Name*</td><td>*Encoding*</td><td>*Description*</td><tr>
    <tr><td>demographic</td><td>gender</td><td>string</td><td>The customer's gender</td><tr>
    <tr><td>demographic</td><td>zipcode</td><td>string</td><td>The customer's zipcode</td><tr>
    <tr><td>account</td><td>mthly_spend</td><td>double</td><td>The customer's account spend in the last month</td><tr>

Namespaces are used as a data partitioning mechanism internally. Generally, attributes that are related should be contained in the same namespace.

Note that the source-of-truth for a dictionary is not the Ivory repository itself. The dictionary is typically maintained in a text file (under version control) or a database. An updated dictionary can be imported into a repository at any time.

## Derived features

The dictionary also declares derived (virtual) features by defining expressions against base facts:

&lt;Name&gt;&lt;Source&gt;&lt;Expression&gt;&lt;Window&gt;

For example:

*Table of example expressions*

Virtual features are computed lazily when features are extracted.

## Example dictionary format
