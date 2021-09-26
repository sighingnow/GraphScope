package com.alibaba.grape;

import com.alibaba.grape.jobConf.JobConf;
import com.alibaba.grape.sample.sssp.SSSPMirror;
import com.alibaba.grape.sample.sssp.SSSPMirrorDefaultContext;
import com.alibaba.grape.sample.sssp.mirror.SSSPEdata;
import com.alibaba.grape.sample.sssp.mirror.SSSPOid;
import com.alibaba.grape.sample.sssp.mirror.SSSPVdata;

public class SSSPMirrorMainClass {
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
            job.setAppClass(SSSPMirror.class.getName());
            job.setAppContextClass(SSSPMirrorDefaultContext.class.getName());
            job.setOidType(SSSPOid.class.getName());
            job.setVidType(Long.class.getName());
            job.setVdataType(SSSPVdata.class.getName());
            job.setEdataType(SSSPEdata.class.getName());

            //job.setMessageTypes(Message.class.getName());


            // job.setELineParserClassName(SSSP.TwitterELineParser.class.getName());
            // job.setVLineParserClassName(SSSP.TwitterVLineParser.class.getName());
            // job.setEVLineParserClassName(DatagenLineParser.class.getName());
            job.setEVLineParserClassName(SSSPMirror.MirrorLineParser.class.getName());
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
