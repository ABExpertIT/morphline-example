package com.excelsys.morphlines;


import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Notifications;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;


/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws Exception {
        File configFile = new File("morphline.conf");
        MorphlineContext context = new MorphlineContext.Builder().build();
        Command morphline = new Compiler().compile(configFile, null, context, null);

        // process each input data file
        Notifications.notifyBeginTransaction(morphline);

        {
            InputStream in = new FileInputStream(new File("sample.json"));
            Record record = new Record();
            record.put(Fields.ATTACHMENT_BODY, in);
            morphline.process(record);
            in.close();
        }

        Notifications.notifyCommitTransaction(morphline);
    }
}
