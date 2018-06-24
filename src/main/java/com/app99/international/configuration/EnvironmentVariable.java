package com.app99.international.configuration;


public abstract class EnvironmentVariable {

    static {
        System.setProperty("QUEUE_ENDPOINT", "https://queue.amazonaws.com/492822123016/");
        System.setProperty("QUEUE_NAME", "international-impala");

        System.setProperty("MYSQL_PASS", "#99taxis#");
        System.setProperty("MYSQL_USER", "cloudera");
        System.setProperty("MYSQL_URL", "jdbc:mysql://127.0.0.1:3306/hivedb_k1iiujc5fnqaj3r1akldljulni");

        System.setProperty("PG_PASS", "MX3Mpd9NsBAd22PU9BmAX7*AqxRgrbGZjGyGKNt5");
        System.setProperty("PG_USER", "admindw");
        System.setProperty("PG_DATABASE", "dw");
        System.setProperty("PG_URL", "jdbc:redshift://dw.cthopyfgalif.us-east-1.redshift.amazonaws.com:5439/dw");

        System.setProperty("IMPALA_URL", "jdbc:impala://127.0.0.1:21051/new_app");
        System.setProperty("JDBC_DRIVER", "com.cloudera.impala.jdbc41.Driver");
    }
}
