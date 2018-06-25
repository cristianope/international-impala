package com.app99.international.service.impl;

import com.app99.international.integration.impl.HiveMetastoreDAOImpl;
import com.app99.international.integration.impl.ImpalaDAOImpl;
import com.app99.international.integration.impl.RedshiftDAOImpl;
import com.app99.international.model.Field;
import com.app99.international.model.OptionField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;


public abstract class BasicCommands{

   private static final Logger LOGGER = LoggerFactory.getLogger(BasicCommands.class);


    @Autowired
    private HiveMetastoreDAOImpl catalog;

    @Autowired
    private ImpalaDAOImpl impalaD;

    @Autowired
    private RedshiftDAOImpl redshift;

    public final static String REDSHIFT = "redshift";
    public final static String NEW_APP = "new_app";
    public final static String BACKFILL = "backfill";



    public HiveMetastoreDAOImpl getCatalog() {
        return catalog;
    }

    public ImpalaDAOImpl getImpalaD() {
        return impalaD;
    }

    public RedshiftDAOImpl getRedshift() {
        return redshift;
    }

    public static Logger getLOGGER() {
        return LOGGER;
    }

    protected boolean hasPartitions(String database, String tableName) throws Exception {
        return (database.equals(BACKFILL)) ? true : catalog.getFieldsPartitions(database, tableName).size() != 0 ? true : false;
    }


    protected String getPartitions(String separator, String database, String tableName) throws Exception {
        String partitions = getPartitions(separator, database, tableName, null, OptionField.DEFAULT);
        return (partitions.length() == 0)  ? getPartitions(separator, tableName, null, catalog.getFieldsPartitionsFile(tableName), OptionField.DEFAULT): partitions;
    }
    protected String getPartitions(String separator, String database, String tableName, String[] values, OptionField optionShowField) throws Exception {
        return getPartitions(separator, tableName, values, catalog.getFieldsPartitions(database, tableName), optionShowField);
    }

    protected String getPartitions(String separator, String tableName, String[] values, List<Field> fields, OptionField optionShowField) throws Exception{
        StringBuffer command = new StringBuffer();

        int size = fields.size();
        for (int i = 0; i < fields.size(); i++) {

            switch (optionShowField) {
                case ONLY_FIELDS:
                    command.append(values[i]);
                    break;
                case ONLY_VALUES:
                    command.append(fields.get(i).getField());
                    break;
                case FIELD_EQUAL_VALUE:
                    command.append(fields.get(i).getField() + "=" + values[i]);
                    break;
                default:
                    command.append(fields.get(i).toParquet(tableName));
            }

            if (--size != 0) {
                command.append(separator);
            }
        }
        return command.toString();
    }



    protected String getParameters() {
        return "SET compression_codec=snappy; SET parquet_file_size=256mb; ";
    }

    private String prepareFieds(String tableName, List<Field> fields, boolean ddl) throws Exception{
        StringBuffer command = new StringBuffer("");
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

    private String prepareHasPartitions(String tableName, List<Field> fields, boolean ddl, boolean externalTable) throws Exception {
        StringBuffer properties = new StringBuffer();
        if(externalTable && ddl){
            properties.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ");
        }
        return fields.isEmpty() ? (ddl ? properties.toString() : " ") : "PARTITIONED BY (" + prepareFieds(tableName, fields, ddl) + ") " + (ddl ? properties.toString() : " ");
    }

    protected String getFields(String database, String tableName, boolean useRedshiftSource) throws Exception{
        return prepareFieds(tableName, existTable(database,tableName, useRedshiftSource), false);
    }

    protected String getFieldsOrderByPartitions(String database, String tableName, boolean cast) throws Exception {
        return prepareFieds(tableName, existTable(database, tableName, false, true), !cast);
    }

    protected List<Field> existTable(String database, String tableName, boolean useRedshiftSource) throws Exception {
        return existTable(database, tableName, useRedshiftSource, true);
    }

    protected List<Field> existTable(String database, String tableName, boolean useRedshiftSource, boolean ddl) throws Exception {
        return useRedshiftSource ? redshift.getFields(database, tableName) : (ddl) ? catalog.getFieldsDDL(database, tableName) : catalog.getFields(database, tableName);
    }


    protected String createTable(String database, String tableName, boolean externalTable, List<Field> fields) throws Exception {
        LOGGER.info("createQuery ======================== " + database + " - " + tableName + " - " + externalTable);

        StringBuffer command = new StringBuffer("CREATE " + (externalTable ? "EXTERNAL" : "") + " TABLE IF NOT EXISTS " + database + "." + tableName + " (" );

        command.append(prepareFieds(tableName, fields, true)  + ") ");
        command.append(externalTable ? prepareHasPartitions(tableName, catalog.getFieldsPartitions(REDSHIFT, tableName), true, externalTable) : prepareHasPartitions(tableName, catalog.getFieldsPartitionsFile(tableName), true, externalTable) );

        if(externalTable){
            command.append("LOCATION 's3a://99taxis-dw-international-online/hive-export/international/" + tableName + "/' ");
            command.append("TBLPROPERTIES(\"skip.header.line.count\"=\"1\")");
        }else{
            command.append("STORED AS PARQUET");
        }
        return command.toString() + "; ";
    }
}