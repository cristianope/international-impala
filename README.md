# Project Chameleon

Chameleon is a open source that read a source - only Redshift - and understand how structure was built, after that - based in rules - the a new structure is created at Impala/Hive.
Two databases are created in Impala/Hive:
* S3 format - This is a first structure created, this database reach data in S3, normally where data it.
* Parquet format - This is a format more specify and must be used when want execute queries.

  This application depends SQS/SNS, this Chameleon listen a queue and does him job. So, It's necessary to create a event in aws when a actions happen.

## Maven

This project use Maven, so it's necessary have a some especify libraries.
* mvn clean test - Execute tests (To run a test is necessary change the variables in EnvironmentVariable Class (test folder))
* mvn clean install - Execute tests and create a jar file.

It's necessary to configure environment variables:

```
export QUEUE_ENDPOINT=https://queue.amazonaws.com/492822123016/
export QUEUE_NAME=international-impala

export IMPALA_URL="jdbc:impala://127.0.0.1:21051/default;UseNativeQuery=1;"
export JDBC_DRIVER = com.cloudera.impala.jdbc41.Driver

export MYSQL_PASS="PASS"
export MYSQL_USER="USER"
export MYSQL_URL="jdbc:mysql://127.0.0.1:3306/hivedb_k1iiujc5fnqaj3r1akldljulni"

export PG_PASS="PASSWORD"
export PG_USER=USER
export PG_URL="jdbc:redshift://localhost:5439/dw"
```

### Prerequisites

Some libraries are necessaries to the application works. This libraries are describe in pom.xml


```
 <dependency>
            <groupId>tcliservice</groupId>
            <artifactId>tcliservice</artifactId>
            <version>1.0</version>
            <scope>system</scope>
            <systemPath>${basedir}/BOOT-INF/lib/TCLIServiceClient.jar</systemPath>
        </dependency>
        <dependency>
            <groupId>thrift</groupId>
            <artifactId>linthrift</artifactId>
            <version>1.0</version>
            <scope>system</scope>
            <systemPath>${basedir}/BOOT-INF/lib/libthrift-0.9.0.jar</systemPath>
        </dependency>
        <dependency>
            <groupId>libfb</groupId>
            <artifactId>fb303</artifactId>
            <version>1.0</version>
            <scope>system</scope>
            <systemPath>${basedir}/BOOT-INF/lib/libfb303-0.9.0.jar</systemPath>
        </dependency>
        <dependency>
            <groupId>impala</groupId>
            <artifactId>jdbc41</artifactId>
            <version>1.0</version>
            <scope>system</scope>
            <systemPath>${basedir}/BOOT-INF/lib/impalaJDBC41.jar</systemPath>
        </dependency>
        <dependency>
            <groupId>hive</groupId>
            <artifactId>metastore</artifactId>
            <version>1.0</version>
            <scope>system</scope>
            <systemPath>${basedir}/BOOT-INF/lib/hive_metastore.jar</systemPath>
        </dependency>
        <dependency>
            <groupId>hive</groupId>
            <artifactId>service</artifactId>
            <version>1.0</version>
            <scope>system</scope>
            <systemPath>${basedir}/BOOT-INF/lib/hive_service.jar</systemPath>
        </dependency>

```

### Run in Debug mode

If you run in debug mode, you can do this way in your terminal.

```
java -Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=8001,suspend=y -jar target/international-1.0-SNAPSHOT.jar

```
After that, you can run you project at IntelliJ in debug mode.
Remember that is necessary to configure a remote execute.

Ex.: -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8001
