# Installing Ivory

Set up these requirements:

## Java

Java must be installed, and JAVA_HOME must be set.

Recommended Java versions for Hadoop are listed at [http://wiki.apache.org/hadoop/HadoopJavaVersions](http://wiki.apache.org/hadoop/HadoopJavaVersions).

## SSH

ssh must be installed and sshd must be running to use the Hadoop scripts that manage remote Hadoop daemons.

## Hadoop

Download Hadoop from [http://www.apache.org/dyn/closer.cgi/hadoop/common/](http://www.apache.org/dyn/closer.cgi/hadoop/common/).

Unpack the downloaded Hadoop distribution to `usr/local/hadoop`. In the distribution, edit  `etc/hadoop/hadoop-env.sh` to add:

    # Assuming your installation directory is /usr/local/hadoop
    export HADOOP_PREFIX=/usr/local/hadoop

Check the Hadoop installation by running:

    $ hadoop version

You should see something like:

    Hadoop 2.6.0

Now set HADOOP_HOME in `~/.profile`:

    HADOOP_HOME=/usr/local/hadoop
    export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

and run:

    $ source ~/.profile

Check HADOOP_HOME by running:

    $ echo $HADOOP_HOME

You should see something like:

    /usr/local/hadoop

## Ivory

Install Ivory by running:

    $ curl -OfsSL https://raw.githubusercontent.com/ambiata/ivory/master/bin/install
    $ chmod a+x install
    $ ./install /usr/local/ivory

Add Ivory to the system path:

    # ivory home
    export PATH=$PATH:/usr/local/ivory/bin

Check the Ivory install:

    $ ivory —-help

You should see something like:

    Ivory 1.0.0-cdh5-20141113112607-b49d4ba
    Usage: {rename|cat-dictionary|cat-errors|cat-facts|chord|config|convert-dictionary|count-facts|create-repository|debug-dump-facts|debug-dump-reduction|fact-diff|import-dictionary|ingest|recompress|recreate|snapshot|factset-statistics}
