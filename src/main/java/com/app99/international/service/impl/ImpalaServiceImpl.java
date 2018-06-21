package com.app99.international.service.impl;

import com.app99.international.dao.impl.HiveMetastoreDAOImpl;
import com.app99.international.dao.impl.ImpalaDAOImpl;
import com.app99.international.model.Field;
import com.app99.international.service.ImpalaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class ImpalaServiceImpl implements ImpalaService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImpalaServiceImpl.class);

    @Autowired
    private HiveMetastoreDAOImpl catalog;

    @Autowired
    private ImpalaDAOImpl impalaD;

    @Value("${CONNECTION_URL}")
    private static String connectionUrl;

    @Value("${JDBC_DRIVER}")
    private static String jdbcDriverName;

    @Override
    public String prepareCommand(String oldDatabase, String database, String tableName, String year, String month, String day, String hour) throws Exception{
        StringBuffer command = new StringBuffer("");
        boolean hasPartitions = hasPartitions(tableName);
        boolean hasIsFullPartition = catalog.isFullPartition(tableName);

        if(hasIsFullPartition){
            LOGGER.info("isFullPartition - Add TRUNCATE command: " + database + "." + tableName);
            command.append("TRUNCATE " + database + "." + tableName + "; ");
        }else{
            if(hasPartitions) {
                command.append("ALTER TABLE " + database + "." + tableName + " DROP PARTITION (" + getPartitions(",", tableName, year, month, day, hour) + "); ");
                LOGGER.info("DROP PARTITION - command: " + command.toString());
            }
        }

        command.append(getParameters());

        command.append("INSERT INTO " + database + "." + tableName + " ");
        command.append("SELECT ");

        if(tableName.startsWith("dim_")){
            LOGGER.info("dim_ found - Add DISTINCT command: " + database + "." + tableName);
            command.append("DISTINCT ");
        }

        command.append(getFields(tableName));
        command.append(" FROM " + oldDatabase + "." + tableName + " ");

        if(hasPartitions) {
            command.append("WHERE " + getPartitions(" AND ", tableName, year, month, day, hour));
        }

        command.append("; ");

        if(hasIsFullPartition){
            command.append("COMPUTE STATS " + database + "." + tableName + "; ");
        }else{
            command.append("COMPUTE INCREMENTAL STATS " + database + "." + tableName + " PARTITION(" + getPartitions(",", tableName, year, month, day, hour) + "); ");
        }

        LOGGER.info("Final command: " + command.toString());

        return command.toString();
    }

    private boolean hasPartitions(String tableName) {
        return catalog.getFieldsPartitions("redshift", tableName).size() != 0 ? true : false;
    }

    private String getPartitions(String separator, String tableName, String year, String month, String day, String hour) {
        StringBuffer command = new StringBuffer();
        String[] values = {year, month, day, hour};
        List<Field> fields = catalog.getFieldsPartitions("redshift", tableName);

        int size = fields.size();
        for (int i = 0; i < fields.size(); i++) {
            command.append(fields.get(i).getField() + "=" + values[i]);

            if (--size == 0){
                command.append(separator);
            }
        }
        LOGGER.info("Command: " + command.toString());

        return command.toString();
    }

    private String getParameters() {
        return "set compression_codec=snappy; set parquet_file_size=256mb; ";
    }

    @Override
    public boolean executeCommand(String command) throws Exception{
        return true; // impalaD.executeCommand(command);
    }

    private String getFields(String tableName) {
        StringBuffer command = new StringBuffer();
        List<Field> fields = catalog.getFields("redshift", tableName);

        int size = fields.size();
        for (Field field:fields) {
            command.append(field.toParquet());

            if (--size == 0){
                command.append(",");
            }
        }

        return command.toString();
    }

    @Override
    public  boolean AddPartitionsS3(String oldDatabase, String tableName, String year, String month, String day, String hour) throws Exception {
        StringBuffer partition = new StringBuffer("ALTER TABLE " + oldDatabase + "." + tableName + " ADD PARTITION(" + getPartitions(",", tableName, year, month, day, hour) + ");");
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
        LOGGER.info("Add Partition: " + partition.toString());

        executeCommand(partition.toString());

        return true;
    }


}