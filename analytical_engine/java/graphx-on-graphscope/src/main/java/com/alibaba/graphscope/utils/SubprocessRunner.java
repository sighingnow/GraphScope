package com.alibaba.graphscope.utils;

import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.FileChannel;

class StreamCopier implements Runnable {
    private InputStream in;
    private OutputStream out;

    public StreamCopier(InputStream in, OutputStream out) {
        this.in = in;
        this.out = out;
    }

    public void run() {
        try {
            int n;
            byte[] buffer = new byte[4096];
            while ((n = in.read(buffer)) != -1) {
                out.write(buffer, 0, n);
                out.flush();
            }
        }
        catch (IOException e) {
            System.out.println(e);
        }
    }
}

class InputCopier implements Runnable {
    private FileChannel in;
    private OutputStream out;

    public InputCopier(FileChannel in, OutputStream out) {
        this.in = in;
        this.out = out;
    }

    public void run() {
        try {
            int n;
            ByteBuffer buffer = ByteBuffer.allocate(4096);
            while ((n = in.read(buffer)) != -1) {
                out.write(buffer.array(), 0, n);
                out.flush();
            }
            out.close();
        }
        catch (AsynchronousCloseException e) {}
        catch (IOException e) {
            System.out.println(e);
        }
    }
}
public class SubprocessRunner {
     static FileChannel getChannel(InputStream in)
        throws NoSuchFieldException, IllegalAccessException {
        Field f = FilterInputStream.class.getDeclaredField("in");
        f.setAccessible(true);
        while (in instanceof FilterInputStream)
            in = (InputStream)f.get((FilterInputStream)in);
        return ((FileInputStream)in).getChannel();
    }
}
