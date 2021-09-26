package com.alibaba.grape;

import com.alibaba.grape.jobConf.JobConf;
import com.alibaba.grape.parallel.message.DoubleMsg;
import com.alibaba.grape.sample.lineparser.Graph500LineParser;

import java.io.FileOutputStream;
import java.util.Properties;

public class PageRankGraph500MainClass {
    public static void main(String[] args) {
        for (int i = 0; i < args.length; ++i) {
            System.out.println(i + " " + args[i]);
        }
        if (args.length != 2) {
            System.out.println(
                    "Usage: <vfile> <efile>");
            System.exit(-1);
        }
        try {
            JobConf job = new JobConf();
//            job.addInput(TableInfo.builder()
//                    .tableName(args[0])
//                    .loaderClassName(SSSPDefault.SSSPLoader.class.getName())
//                    .build()); // loaderClass(SSSPLoader.class)
//            job.addOutput(TableInfo.builder().tableName(args[1]).label("pagerank_output").build());
            job.setVfilePath(args[0]);
            job.setEfilePath(args[1]);
            job.setEVLineParserClassName(Graph500LineParser.class.getName());

            job.setOidType(Long.class.getName());
            job.setVidType(Long.class.getName());
            job.setVdataType(Long.class.getName());
            job.setEdataType(Double.class.getName());
            job.setMessageTypes(DoubleMsg.class, Double.class);

            // alibaba ffi accept java properties input
            FileOutputStream out = new FileOutputStream("/tmp/demo.properties");

            Properties configProperties = new Properties();
            configProperties.setProperty("vidType", Long.class.getName());
            configProperties.setProperty("oidType", Long.class.getName());
            configProperties.setProperty("edataType", Long.class.getName());
            configProperties.setProperty("vdataType", Double.class.getName());
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