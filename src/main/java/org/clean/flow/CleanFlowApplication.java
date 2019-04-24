package org.clean.flow;

import org.clean.flow.entity.Config;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Timer;
import java.util.TimerTask;

public class CleanFlowApplication {

    public static void main(String[] args) throws Exception {
        try {
            Config config = Config.getInstance().installConfig();
            new Timer().schedule(new TimerTask() {
                public void run() {
                    long startTime = System.currentTimeMillis();
                    CleanFlow cleanFlow = new CleanFlow();
                    cleanFlow.setUp();
                    System.err.println(System.currentTimeMillis() - startTime);
                }
            }, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(config.getStartTime()), Long.parseLong(config.getPeriod()));
            StringBuilder builder = new StringBuilder();
            builder.append("\n");
            builder.append("----------------------------------------\n");
            builder.append("|                                      |\n");
            builder.append("|                                      |\n");
            builder.append("|   CleanFlowApplication is running!   |\n");
            builder.append("|                                      |\n");
            builder.append("|                                      |\n");
            builder.append("----------------------------------------\n");
            System.err.println(builder.toString());
        } catch (ParseException e) {
        }
    }

}
