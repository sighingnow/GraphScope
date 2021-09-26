package com.alibaba.grape;

import com.alibaba.grape.jobConf.JobConf;
import com.alibaba.grape.sample.lineparser.MirrorLineParser;
import com.alibaba.grape.sample.types.Edata;
import com.alibaba.grape.sample.types.Message;
import com.alibaba.grape.sample.types.Oid;
import com.alibaba.grape.sample.types.Vdata;

import javax.xml.ws.handler.MessageContext;
import java.io.FileOutputStream;
import java.util.Properties;

public class MessageMirrorMainClass {
    public static void main(String[] args) {
        for (int i = 0; i < args.length; ++i) {
            System.out.println(i + " " + args[i]);
        }
        if (args.length != 2) {
            System.out.println(
                    "Usage: <vfile> <efile> ");
            System.exit(-1);
        }
        try {
            JobConf job = new JobConf();
//            job.addInput(TableInfo.builder()
//                    .tableName(args[0])
//                    .loaderClassName(SSSPDefault.SSSPLoader.class.getName())
//                    .build()); // loaderClass(SSSPLoader.class)
//            job.addOutput(TableInfo.builder().tableName(args[1]).label("sssp_output").build());
            job.setVfilePath(args[0]);
            job.setEfilePath(args[1]);
            job.setAppClass(Message.class.getName());
            job.setAppContextClass(MessageContext.class.getName());
            job.setOidType(Oid.class.getName());
            job.setVidType(Long.class.getName());
            job.setVdataType(Vdata.class.getName());
            job.setEdataType(Edata.class.getName());
            job.setMessageTypes(Message.class);


            // job.setELineParserClassName(SSSP.TwitterELineParser.class.getName());
            // job.setVLineParserClassName(SSSP.TwitterVLineParser.class.getName());
            // job.setEVLineParserClassName(DatagenLineParser.class.getName());
            job.setEVLineParserClassName(MirrorLineParser.class.getName());
            // alibaba ffi accept java properties input
            FileOutputStream out = new FileOutputStream("/tmp/demo.properties");

            Properties configProperties = new Properties();
            configProperties.setProperty("vidType", Long.class.getName());
            configProperties.setProperty("oidType", Oid.class.getName());
            configProperties.setProperty("vdataType", Vdata.class.getName());
            configProperties.setProperty("edataType", Edata.class.getName());
            //configProperties.setProperty("messageTypes", String.join(":", Double.class.getName(), Long.class.getName()));
            configProperties.setProperty("msgTypes", String.join(",",
                    "sample::Message=" + Message.class.getName(), "sample::Oid=" + Oid.class.getName()));
            configProperties.store(out, "Demo graph type");

            out.close();
            // LOG.info("tables :[" + args[0] + " " + args[1] + "]");
            // LOG.info("ready to write graphjob");
            job.run();
            System.out.println("exiting main function..");
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
}
