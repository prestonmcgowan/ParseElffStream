package com.gist.github.yfnick;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* GZip Util.
*/
public class Gzip {
    private static final Logger log = LoggerFactory.getLogger(Gzip.class);

    /**
     * Private constructor, because Lint told me to.
     */
    private Gzip() {

    }

    /**
    * GZip.
    * https://gist.github.com/yfnick/227e0c12957a329ad138
    * @param data
    * @return
    * @throws IOException
    */
    public static byte[] compress(String data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length());
        GZIPOutputStream gzip = new GZIPOutputStream(bos);
        gzip.write(data.getBytes());
        gzip.close();
        byte[] compressed = bos.toByteArray();
        bos.close();
        return compressed;
    }

    /**
    * Decompress GZip.
    *
    * @param compressed
    * @return
    * @throws IOException
    */
    public static String decompress(byte[] compressed) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
        GZIPInputStream gis = new GZIPInputStream(bis);
        BufferedReader br = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line).append(System.getProperty("line.separator"));
        }
        br.close();
        gis.close();
        bis.close();
        return sb.toString();
    }

    /**
    * Is the magic byte there?
    * https://stackoverflow.com/questions/30507653/how-to-check-whether-file-is-gzip-or-not-in-java
    * @param in
    * @return
    */
    public static boolean isGZipped(InputStream in) {
        if (!in.markSupported()) {
            in = new BufferedInputStream(in);
        }
        in.mark(2);
        int magic = 0;
        try {
            magic = in.read() & 0xff | ((in.read() << 8) & 0xff00);
            in.reset();
        } catch (IOException e) {
            log.error("isGZipped error", e);
            return false;
        }
        return magic == GZIPInputStream.GZIP_MAGIC;
    }

    /**
    * Helper function.
    * @param compressed
    * @return
    */
    public static boolean isGZipped(byte[] compressed) {
        return isGZipped(new ByteArrayInputStream(compressed));
    }
}
