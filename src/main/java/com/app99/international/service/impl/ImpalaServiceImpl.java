package com.app99.international.service.impl;

import com.app99.international.model.Field;
import com.app99.international.service.ImpalaService;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class ImpalaServiceImpl extends BasicCommands implements ImpalaService{


    @Override
    public String prepareCommand(String oldDatabase, String database, String tableName, String year, String month, String day, String hour) throws Exception{
        StringBuffer command = new StringBuffer("");
        boolean hasPartitions = hasPartitions(tableName);
        boolean hasIsFullPartition = getCatalog().isFullPartition(tableName);

        List<Field> fields = existTable(database, tableName, false);
        if(fields.size() == 0){
            command.append(createTable(database, tableName, false, existTable(oldDatabase, tableName, false)));
        }

        if(hasIsFullPartition){
            getLOGGER().info("isFullPartition - Add TRUNCATE command: " + database + "." + tableName);
            command.append("TRUNCATE " + database + "." + tableName + "; ");
        }else{
            if(hasPartitions) {
                command.append("ALTER TABLE " + database + "." + tableName + " DROP PARTITION (" + getPartitions(",", tableName, year, month, day, hour) + "); ");
                getLOGGER().info("DROP PARTITION - command: " + command.toString());
            }
        }

        command.append(insertCommand(database, oldDatabase, tableName, new String[]{year, month, day, hour}, hasPartitions));

        if(hasIsFullPartition){
            command.append("COMPUTE STATS " + database + "." + tableName + "; ");
        }else{
            command.append("COMPUTE INCREMENTAL STATS " + database + "." + tableName + " PARTITION(" + getPartitions(",", tableName, year, month, day, hour) + "); ");
        }

        getLOGGER().info("Final command: " + command.toString());

        return command.toString();
    }

    private String insertCommand(String database, String oldDatabase, String tableName, String[] date, boolean hasPartitions) throws Exception {
        StringBuffer command = new StringBuffer(getParameters());

        command.append("INSERT INTO " + database + "." + tableName + " ");
        command.append("SELECT ");

        if(tableName.startsWith("dim_")){
            getLOGGER().info("dim_ found - Add DISTINCT command: " + database + "." + tableName);
            command.append("DISTINCT ");
        }

        command.append(getFields(oldDatabase, tableName, false));
        command.append(" FROM " + oldDatabase + "." + tableName + " ");

        if(hasPartitions) {
            command.append("WHERE " + getPartitions(" AND ", tableName, date[0], date[1], date[2], date[3]));
        }

        command.append("; ");

        return command.toString();
    }

    @Override
    public boolean backfillTable(String tableName) throws Exception {
        return executeCommand("SET mem_limit=64g; "+ insertCommand("new_app", "backfill", tableName, null, false));
    }

    @Override
    public boolean executeCommand(String command) throws Exception{
        return true; // impalaD.executeCommand(command);
    }

    @Override
    public  boolean AddPartitionsS3(String oldDatabase, String tableName, String year, String month, String day, String hour) throws Exception {
        StringBuffer partition = new StringBuffer("ALTER TABLE " + oldDatabase + "." + tableName + " ADD PARTITION(" + getPartitions(",", tableName, year, month, day, hour) + ") ");
        partition.append("LOCATION 's3a://99taxis-dw-international-online/hive-export/international/" + tableName + "/" + getPartitions("/", tableName, year, month, day, hour, true) + "/'; ");
        partition.append("COMPUTE INCREMENTAL STATS " + oldDatabase + "." + tableName + " PARTITION(" + getPartitions(",", tableName, year, month, day, hour) + ");");

      /*  StringBuffer partition = new StringBuffer("ALTER TABLE " + oldDatabase + "." + tableName + " ADD PARTITION(year="+ year +",month=" + month + ",day=" + day);
        StringBuffer computeStats = new StringBuffer("COMPUTE INCREMENTAL STATS " + oldDatabase + "." + tableName + " PARTITION(year=" + year + ",month=" + month + ",day=" + day);

        if (hour.length() > 3){
            partition.append(") LOCATION 's3a://99taxis-dw-international-online/hive-export/international/" + tableName + "/" + year + "/" + month + "/" + day + "/'; ");
            computeStats.append("); ");
        }else{
            partition.append(",hour=" + hour + ") LOCATION 's3a://99taxis-dw-international-online/hive-export/international/" + tableName + "/" + year + "/" + month + "/" + day + "/" + hour + "/'; ");
            computeStats.append(",hour=" + hour + "); ");
        }
*/
        getLOGGER().info("Add Partition: " + partition.toString());

        List<Field> fields = existTable("new_app", tableName, true);
        if(fields.size() == 0){
            partition.append(createTable(oldDatabase, tableName, true, fields));
        }

        executeCommand(partition.toString());
        return true;
    }


}