package com.app99.international.service.impl;

import com.app99.international.dao.impl.HiveMetastoreDAOImpl;
import com.app99.international.dao.impl.PostgreSQLDAOImpl;
import com.app99.international.model.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;


public abstract class BasicCommands {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicCommands.class);

    @Autowired
    private HiveMetastoreDAOImpl catalog;

/*    @Autowired
    private ImpalaDAOImpl impalaD;
*/
    @Autowired
    private PostgreSQLDAOImpl redshift;


    public HiveMetastoreDAOImpl getCatalog() {
        return catalog;
    }

/*    public ImpalaDAOImpl getImpalaD() {
        return null; //impalaD;
    }
*/
    public PostgreSQLDAOImpl getRedshift() {
        return redshift;
    }

    public static Logger getLOGGER() {
        return LOGGER;
    }

    protected boolean hasPartitions(String tableName) {
        return catalog.getFieldsPartitions("redshift", tableName).size() != 0 ? true : false;
    }

    protected String getPartitions(String separator, String tableName, String year, String month, String day, String hour){
        return getPartitions(separator, tableName, year, month, day, hour, false);
    }

    protected String getPartitions(String separator, String tableName, String year, String month, String day, String hour, boolean onlyField) {
        StringBuffer command = new StringBuffer();
        String[] values = {year, month, day, hour};
        List<Field> fields = catalog.getFieldsPartitions("redshift", tableName);

        int size = fields.size();
        for (int i = 0; i < fields.size(); i++) {

            if(onlyField){
                command.append(values[i]);
            }else{
                command.append(fields.get(i).getField() + "=" + values[i]);
            }

            if (--size != 0){
                command.append(separator);
            }
        }
        return command.toString();
    }

    protected String getParameters() {
        return "SET compression_codec=snappy; SET parquet_file_size=256mb; ";
    }

    private String prepareFieds(String tableName, List<Field> fields, boolean ddl) throws Exception{
        StringBuffer command = new StringBuffer();
        int size = fields.size();

        for (int i = 0; i < fields.size(); i++) {
            fields.get(i).updateFieldsNewDataTypes(tableName);
            if(ddl){
                command.append(fields.get(i).toCreateDDL(tableName));
            }else{
                command.append(fields.get(i).toParquet(tableName));
            }

            if (--size != 0){
                command.append(",");
            }
        }

        return command.toString();
    }

    protected String getFields(String schema, String tableName, boolean useRedshiftSource) throws Exception{
        return prepareFieds(tableName, existTable(schema,tableName, useRedshiftSource), false);
    }

    protected List<Field> existTable(String database, String tableName, boolean useRedshiftSource) throws Exception {
        return useRedshiftSource ? redshift.getFields(database, tableName) : catalog.getFields(database, tableName);
    }

    protected String createTable(String database, String tableName, boolean externalTable, List<Field> fields) throws Exception {
        StringBuffer command = new StringBuffer("CREATE " + (externalTable ? "EXTERNAL" : "") + " TABLE " + database + "." + tableName + " IF NOT EXISTS (" );
        command.append(prepareFieds(tableName, fields, true) + ") ");

        if(externalTable){
            command.append("PARTITIONED BY (" + prepareFieds(tableName, catalog.getFieldsPartitions("redshift", tableName), true) + ") ");
        }else{
            command.append("PARTITIONED BY (" + prepareFieds(tableName, catalog.getFieldsPartitionsFile(tableName), true) + ") ");
        }


        if(externalTable){
            // todo
            command.append("LOCATION 's3a://99taxis-dw-international-online/hive-export/international/" + tableName + "/'; ");

            //        TBLPROPERTIES ('key1'='value1', 'key2'='value2', ...)
        }else{
            command.append("STORED AS PARQUET; ");
        }

        return command.toString();
    }




}