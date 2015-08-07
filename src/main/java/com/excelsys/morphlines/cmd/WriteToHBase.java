package com.excelsys.morphlines.cmd;

import com.typesafe.config.Config;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;

import java.util.Arrays;
import java.util.Collection;

public class WriteToHBase implements CommandBuilder {
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new WriteToHBaseCommand(this, config, parent, child, context);
    }

    public Collection<String> getNames() {
        return Arrays.asList("writeToHBase");
    }

    public static class WriteToHBaseCommand extends AbstractCommand {
        protected WriteToHBaseCommand(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);
        }

        @Override
        protected boolean doProcess(Record record) {
            int i = 0;

            i++;

            return super.doProcess(record);
        }
    }
}
