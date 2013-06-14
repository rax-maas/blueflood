package com.cloudkick.blueflood.tools.jmx;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JmxQuery {
    private static final String CMD_LIST = "list";
    private static final String CMD_GET = "get";
    private static final String CMD_HELP = "help";
    public static boolean DEBUG = false;
    
    public static void main(String args[]) {
        for (String arg : args)
            if ("-debug".equals(arg))
                JmxQuery.DEBUG = true;
        if (JmxQuery.DEBUG) {
            for (int i = 0; i < args.length; i++)
                System.out.println(String.format("%d:%s", i, args[i]));
        }
        try {
            String cmd = args[0];
            if (CMD_HELP.equals(cmd))
                doHelp(args);
            else if (CMD_GET.equals(cmd)) 
                doGet(args);
            else if (CMD_LIST.equals(cmd))
                doList(args);
            else {
                System.out.println("invalid command: " + args[0]);
                System.exit(-1);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-1);
        }
    }
    
    private static void doHelp(String args[]) {
        String helpCmd = args.length == 1 ? CMD_HELP : args[1];
        if (CMD_HELP.equals(helpCmd)) {
            System.out.println("Gives command-specific help");
            System.out.println("Usage: jmx-list help [command]");
        } else if (CMD_LIST.equals(helpCmd)) {
            System.out.println("Lists objects or attributes on an object");
            System.out.println("Usage: jmx-list list [object]");
        } else if (CMD_GET.equals(helpCmd)) {
            System.out.println("Prints jmx values for a particular object and attributes");
            System.out.println("Usage: jmx-list get [object] [attribute [attribute [attribute] ...]] [--env env] [--hosts host:1,host:2,host:3...host:n]");
        }
    }
    
    private static void doList(String args[]) {
        System.out.println("NOT IMPLEMENTED");
    }
    
    private static void doGet(String args[]) throws Exception {
        
        String object = args[1];
        List<String> attributes = new ArrayList<String>();
        Map<String, String> otherParams = new HashMap<String, String>();
        
        int pos = 2;
        while (pos < args.length && !args[pos].startsWith("--"))
            if (!args[pos].startsWith("-"))
                attributes.add(args[pos++]);
        while (pos < args.length) {
            if (args[pos].startsWith("--"))
                otherParams.put(args[pos++], args[pos++]);
            else
                pos++;
        }
        
        Properties props = getHosts(otherParams.get("--env"));
        props.putAll(otherParams);
        
        // establish a few things
        if (otherParams.containsKey("--dec"))
            FetchAttributesCallable.DECIMAL_FORMAT = new DecimalFormat(otherParams.get("--dec"));
        
        ObjectName objectName = null;
        try {
            objectName = new ObjectName(object);    
        } catch (MalformedObjectNameException ex) {
            ex.printStackTrace();;
            System.exit(-1);
        }
        
        String[] hosts = props.getProperty("hosts").split(",", -1);
        System.out.println("Object: " + object);
        MultiQuery assemble = new MultiQuery(hosts, attributes.toArray(new String[attributes.size()]), objectName);
        try {
            Collection<OutputFormatter> output = OutputFormatter.sort(assemble.assembleBlocking());
            assemble.print(output, System.out);
        } catch (InterruptedException ex) {
            System.err.println("Execution did not finish within 10sec");
            System.exit(-1);
        }
    }
    
    private static final String[] hostFileSearchPaths = {
            System.getProperty("user.home"),
            "/etc",
            "/opt/ele-conf"
    };
    
    private static Properties getHosts(String envOrNull) throws IOException {
        Properties props = new Properties();
        File hostFile = null;
        for (String path : hostFileSearchPaths) {
            File f;
            if (envOrNull == null) {
                String fileName = path.equals(System.getProperty("user.home")) ? ".jmxhosts" : "jmxhosts";
                f = new File(path, fileName);
            } else {
                String prefix = path.equals(System.getProperty("user.home")) ? ".jmxhosts_" : "jmxhosts_";
                f = new File(path, prefix + envOrNull);
            }
            if (f.exists()) {
                if (JmxQuery.DEBUG)
                    System.out.println("Reading hosts from " + f.getAbsolutePath());
                hostFile = f;
                break;
            }
        }
        if (hostFile == null)
            return props;

        props.put("hosts", Joiner.on(",").join(Files.readLines(hostFile, Charsets.UTF_8)));
        
        return props;
    }
    
}
