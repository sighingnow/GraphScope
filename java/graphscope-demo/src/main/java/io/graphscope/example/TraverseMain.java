package io.graphscope.example;

import com.alibaba.grape.jobConf.JobConf;
import com.alibaba.grape.parallel.message.LongMsg;
import com.alibaba.grape.parallel.message.DoubleMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraverseMain {
    public static Logger logger = LoggerFactory.getLogger(TraverseMain.class.getName());

    public static void main(String[] args) {
        if (args.length != 1) {
            logger.error("pls provide the only parameter: job_conf_path");
            return;
        }
        JobConf job = new JobConf(args[0]);

        job.setOidType(Long.class.getName());
        job.setVidType(Long.class.getName());
        job.setVdataType(Long.class.getName());
        job.setEdataType(Double.class.getName()); //set but not used
        job.setMessageTypes(LongMsg.class,DoubleMsg.class);

        job.run();
    }
}
