package com.excelsys.morphlines.cmd;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class WriteToHBase implements CommandBuilder {
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new WriteToHBaseCommand(this, config, parent, child, context);
    }

    public Collection<String> getNames() {
        return Arrays.asList("writeToHBase");
    }

    public static class WriteToHBaseCommand extends AbstractCommand {
        Table table;

        protected WriteToHBaseCommand(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);
        }

        @Override
        protected boolean doProcess(Record record) {
            try {
                if (null == table) {
                    Configuration config = HBaseConfiguration.create();

                    //Add any necessary configuration files (hbase-site.xml, core-site.xml)
                    config.addResource(new Path("/etc/hbase/conf", "hbase-site.xml"));
                    config.addResource(new Path("/etc/hadoop/conf", "core-site.xml"));

                    Connection connection = ConnectionFactory.createConnection(config);

                    this.table = connection.getTable(TableName.valueOf("tweets"));
                }

                doProcessInternal(table, record);
            } catch (Exception exc) {
                throw new RuntimeException(exc);
            }


            return super.doProcess(record);
        }

        private void doProcessInternal(Table t, Record record) throws Exception {
            Put p = new Put(record.get("id").get(0).toString().getBytes());

            for (Map.Entry<String, Object> k : record.getFields().entries()) {
                Object v = k.getValue();

                if (k.getKey().startsWith("tw:"))
                    p.addColumn("tw".getBytes(), k.getKey().substring(3).getBytes(), v.toString().getBytes());
            }

            t.put(p);
        }
    }
}
