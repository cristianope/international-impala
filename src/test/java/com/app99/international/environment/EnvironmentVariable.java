package com.app99.international.environment;


public abstract class EnvironmentVariable {

    static {
        System.setProperty("QUEUE_ENDPOINT", "https://queue.amazonaws.com/492822123016/");
        System.setProperty("QUEUE_NAME", "international-impala");

        System.setProperty("MYSQL_PASS", "#99taxis#");
        System.setProperty("MYSQL_USER", "cloudera");
        System.setProperty("MYSQL_URL", "jdbc:mysql://127.0.0.1:3306/hivedb_k1iiujc5fnqaj3r1akldljulni");

        System.setProperty("PG_PASS", "duI5or608wiYT85gQVKeq5NSddyXqxra");
        System.setProperty("PG_USER", "impala_user");
        System.setProperty("PG_DATABASE", "dw");
        System.setProperty("PG_URL", "jdbc:redshift://127.0.0.1:5439/dw");

        System.setProperty("IMPALA_URL", "jdbc:impala://127.0.0.1:21051/new_app;UseNativeQuery=1;");
        System.setProperty("JDBC_DRIVER", "com.cloudera.impala.jdbc41.Driver");
    }
}
