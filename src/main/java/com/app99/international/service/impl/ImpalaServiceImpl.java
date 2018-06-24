package com.app99.international.service.impl;

import com.app99.international.model.Field;
import com.app99.international.model.OptionField;
import com.app99.international.service.ImpalaService;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class ImpalaServiceImpl extends BasicCommands implements ImpalaService{

    private boolean hasPartitions(String tableName) throws Exception {
        return hasPartitions(REDSHIFT, tableName);
    }


    @Override
    public String prepareCommand(String oldDatabase, String database, String tableName, String year, String month, String day, String hour) throws Exception{
        StringBuffer command = new StringBuffer("");
        boolean hasPartitions = hasPartitions(tableName);
        boolean hasIsFullPartition = getCatalog().isFullPartition(tableName);

        List<Field> fields = existTable(database, tableName, false);

        if(fields.isEmpty()){
            command.append(createTable(database, tableName, false, existTable(oldDatabase, tableName, false)));
        }

       if(hasIsFullPartition && !(fields.isEmpty())){
            getLOGGER().info("isFullPartition - Add TRUNCATE command: " + database + "." + tableName);
            command.append("TRUNCATE " + database + "." + tableName + "; ");
        /*}else{
            if(hasPartitions && !(fields.isEmpty())) {
                command.append("ALTER TABLE " + database + "." + tableName + " DROP PARTITION (" + getPartitions(",", oldDatabase,tableName, new String[]{year, month, day, hour}, OptionField.FIELD_EQUAL_VALUE) + "); ");
                getLOGGER().info("DROP PARTITION - command: " + command.toString());
            }*/
        }

        command.append(createInsertCommand(NEW_APP, tableName, getPartitionsImpalaNewApp(tableName), hasPartitions));
        command.append(createSelectCommand(NEW_APP, oldDatabase, tableName, getPartitionsImpalaRedShift(tableName, new String[] {year,month,day,hour}), hasPartitions));

        if(hasIsFullPartition){
            command.append("COMPUTE STATS " + database + "." + tableName + "; ");
        }else{
            command.append("COMPUTE INCREMENTAL STATS " + database + "." + tableName + " PARTITION(" + getPartitions(",", oldDatabase,tableName, new String[]{year, month, day, hour}, OptionField.FIELD_EQUAL_VALUE) + "); ");
        }

        getLOGGER().info("Final command: " + command.toString());

        return command.toString();
    }

    protected String createInsertCommand(String database, String tableName, String partitions, boolean hasPartitions) throws Exception {
        StringBuffer command = new StringBuffer(getParameters());

        command.append("INSERT INTO " + database + "." + tableName + " ");

        if(hasPartitions){
            command.append("PARTITION (" + partitions +  ") ");
        }

        return command.toString();
    }

    protected String createSelectCommand(String selectDatabase, String fromDatabase, String tableName, String partitions, boolean hasPartitions)throws Exception{
        return createSelectCommand(selectDatabase, fromDatabase, tableName, partitions, hasPartitions, false);
    }

    protected String createSelectCommand(String selectDatabase, String fromDatabase, String tableName, String partitions, boolean hasPartitions, boolean backfill) throws Exception {
        StringBuffer command = new StringBuffer();
        command.append("SELECT ");

        if(tableName.startsWith("dim_")){
            command.append("DISTINCT ");
        }
        if(hasPartitions || backfill){
            command.append(getFieldsOrderByPartitions(selectDatabase, tableName, true));
            String sql = getPartitions(",", selectDatabase, tableName).trim();
            if(sql.length() > 0){
                command.append(", " + sql) ;
            }
        }else{
            command.append(getFields(fromDatabase, tableName, false));
        }
        command.append(" FROM " + fromDatabase + "." + tableName + " ");

        if(hasPartitions) {
            command.append("WHERE " + partitions);
        }

        command.append("; ");

        return command.toString();
    }

    @Override
    public String backfillTable(String tableName) throws Exception {
        StringBuffer command = new StringBuffer();

        command.append(createTable(NEW_APP, tableName, false, existTable(REDSHIFT, tableName, false)));
        command.append(createInsertCommand(NEW_APP, tableName, getPartitionsImpalaNewApp(tableName), hasPartitions(BACKFILL, tableName)));
        command.append(createSelectCommand(NEW_APP,BACKFILL, tableName, " ", false, true));

        return command.toString();
    }

    @Override
    public boolean executeQuery(String command) throws Exception{
        return getImpalaD().executeQuery(command);
    }

    protected String getPartitionsImpalaRedShift(String tableName, String[] values) throws Exception {
        return getPartitions(" AND ", REDSHIFT, tableName, values, OptionField.FIELD_EQUAL_VALUE);
    }

    protected String getPartitionsImpalaNewApp(String tableName) throws Exception{
        return getPartitions(",", NEW_APP, tableName, new String[] {}, OptionField.ONLY_VALUES);
    }



    @Override
    public  String AddPartitionsS3(String oldDatabase, String tableName, String[] values) throws Exception {
        StringBuffer partition = new StringBuffer("ALTER TABLE " + oldDatabase + "." + tableName + " ADD PARTITION(" + getPartitions(",", oldDatabase, tableName, values, OptionField.FIELD_EQUAL_VALUE) + ") ");
        partition.append("LOCATION 's3a://99taxis-dw-international-online/hive-export/international/" + tableName + "/" + getPartitions("/", oldDatabase, tableName, values, OptionField.ONLY_FIELDS) + "/'; ");
        partition.append("COMPUTE INCREMENTAL STATS " + oldDatabase + "." + tableName + " PARTITION(" + getPartitions(",", oldDatabase, tableName, values, OptionField.FIELD_EQUAL_VALUE) + ");");


        getLOGGER().info("AddPartitionsS3 ================= " + partition.toString());

        List<Field> fields = existTable(REDSHIFT, tableName, false, false);
        if(fields.size() == 0){
            partition.append(createTable(oldDatabase, tableName, true, existTable(NEW_APP, tableName, true)));
        }
        return partition.toString();
    }


}