package io.confluent.ps.convert;

import static com.gist.github.yfnick.Gzip.decompress;
import static com.gist.github.yfnick.Gzip.isGZipped;
import static org.apache.kafka.common.serialization.Serdes.String;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.github.jcustenborder.parsers.elf.ElfParser;
import com.github.jcustenborder.parsers.elf.ElfParserBuilder;
import com.github.jcustenborder.parsers.elf.LogEntry;

import com.github.jcustenborder.parsers.elf.parsers.FieldParser;
import com.github.jcustenborder.parsers.elf.parsers.FieldParsers;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class ElffParser {
    static final Map<String, FieldParser> DEFAULT_PARSERS;
    static final Pattern HEADER_PATTERN = Pattern.compile("#([\\S]+):\\s+(.+)");
    static final String HEADER_FIELDS = "Fields";
    private static final Logger log = LoggerFactory.getLogger(ElfParserBuilder.class);

    static {
        Map<String, FieldParser> fieldParsers = new HashMap<>();
        fieldParsers.put("date", FieldParsers.DATE);
        fieldParsers.put("time", FieldParsers.TIME);
        fieldParsers.put("time-taken", FieldParsers.DOUBLE);
        fieldParsers.put("sc-status", FieldParsers.LONG);
        fieldParsers.put("sc-bytes", FieldParsers.LONG);
        fieldParsers.put("cs-bytes", FieldParsers.LONG);
        fieldParsers.put("cs-uri-port", FieldParsers.INT);
//    fieldParsers.put("bytes", FieldParsers.LONG);
        fieldParsers.put("bytes1", FieldParsers.LONG);
        fieldParsers.put("bytes2", FieldParsers.LONG);

        DEFAULT_PARSERS = Collections.unmodifiableMap(fieldParsers);
    }
    final Map<String, FieldParser> fieldParsers = new LinkedHashMap<>();
    Matcher headerMatcher = HEADER_PATTERN.matcher("");



    public static void main (String[] args ) throws IOException{
     Logger log = LoggerFactory.getLogger(ParseElffStream.class);
     List<LogEntry> messages = new ArrayList<>();
     List<Map<String, Object>> other = new ArrayList<>();
     String  input =  "#Software: SGOS 3.2.4.8#Version: 1.0\n#Date: 2005-04-12 19:56:33\r\n#Fields: date time time-taken c-ip sc-status s-action sc-bytes cs-bytes cs-method cs-uri-scheme cs-host cs-uri-path cs-uri-query cs-username s-hierarchy s-supplier-name rs(Content-Type) cs(User-Agent) sc-filter-result sc-filter-category x-virus-id s-ip s-sitename x-virus-details x-icap-error-code x-icap-error-details\r\n2005-04-12 21:03:45 74603 192.16.170.46 503 TCP_ERR_MISS 1736 430 GET http www.yahoo.com / - - NONE 192.16.170.42 - \"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.7.6) Gecko/20050317 Firefox/1.0.2\" DENIED none - 192.16.170.42 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 21:03:45 57358 192.16.170.46 503 TCP_ERR_MISS 1736 617 GET http www.yahoo.com / - - NONE 192.16.170.42 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\" DENIED none - 192.16.170.42 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 21:58:45 74630 192.16.170.45 503 TCP_ERR_MISS 1736 853 GET http www.yahoo.com /p.gif ?t=1113343012&_ylp=A0SOxU0kRFxC_xUBngL1cSkA&hp=0&ct=lan&sh=768&sw=1024&ch=508&cw=803&ni=18&sss=1113343012&t1=1113343001193&d1=1582&d2=1612&d3=3104&d4=3234&d5=24625 - NONE 192.16.170.43 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 192.16.170.43 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:00:18 63358 192.16.170.45 503 TCP_ERR_MISS 1736 854 GET http www.yahoo.com /p.gif ?t=1113343012&_ylp=A0SOxU0kRFxC_xUBngL1cSkA&hp=0&ct=lan&sh=768&sw=1024&ch=508&cw=803&ni=18&sss=1113343012&t1=1113343001193&d1=1582&d2=1612&d3=3104&d4=3234&d5=126281 - NONE 192.16.170.43 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 192.16.170.43 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:00:18 74748 192.16.170.45 503 TCP_ERR_MISS 1736 853 GET http www.yahoo.com /p.gif ?t=1113343012&_ylp=A0SOxU0kRFxC_xUBngL1cSkA&hp=0&ct=lan&sh=768&sw=1024&ch=508&cw=803&ni=18&sss=1113343012&t1=1113343001193&d1=1582&d2=1612&d3=3104&d4=3234&d5=42641 - NONE 192.16.170.43 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 192.16.170.43 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:00:43 74721 192.16.170.45 503 TCP_ERR_MISS 1736 503 GET http www.cnn.com / - - NONE 192.16.170.43 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 192.16.170.43 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:00:43 57859 192.16.170.45 503 TCP_ERR_MISS 1736 503 GET http www.cnn.com / - - NONE 192.16.170.43 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 192.16.170.43 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:00:43 20745 192.16.170.45 503 TCP_ERR_MISS 1736 503 GET http www.cnn.com / - - NONE 192.16.170.43 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 192.16.170.43 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:01:34 74753 192.16.170.45 503 TCP_ERR_MISS 1736 854 GET http www.yahoo.com /p.gif ?t=1113343012&_ylp=A0SOxU0kRFxC_xUBngL1cSkA&hp=0&ct=lan&sh=768&sw=1024&ch=508&cw=803&ni=18&sss=1113343012&t1=1113343001193&d1=1582&d2=1612&d3=3104&d4=3234&d5=118460 - NONE 192.16.170.43 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 192.16.170.43 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:03:18 74827 192.16.170.45 503 TCP_ERR_MISS 1736 342 GET http www.cnn.com / - - NONE 192.16.170.43 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 192.16.170.43 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:03:18 50614 192.16.170.45 503 TCP_ERR_MISS 1736 342 GET http www.cnn.com / - - NONE 192.16.170.43 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 192.16.170.43 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:03:18 61044 192.16.170.45 503 TCP_ERR_MISS 1736 342 GET http www.cnn.com / - - NONE 192.16.170.43 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 192.16.170.43 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:03:18 14506 192.16.170.45 503 TCP_ERR_MISS 1736 342 GET http www.cnn.com / - - NONE 192.16.170.43 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 192.16.170.43 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:03:18 22744 192.16.170.45 503 TCP_ERR_MISS 1736 342 GET http www.cnn.com / - - NONE 192.16.170.43 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 192.16.170.43 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:04:44 74587 192.16.170.45 503 TCP_ERR_MISS 1736 342 GET http www.cnn.com / - - NONE 192.16.170.43 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 192.16.170.43 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:05:19 74767 192.16.170.45 503 TCP_ERR_MISS 1736 814 GET http www.yahoo.com / - - NONE 192.16.170.43 - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 192.16.170.43 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n#Software: SGOS 3.2.4.8\r\n#Version: 1.0\r\n#Date: 2005-04-12 22:40:05\r\n#Fields: date time time-taken c-ip sc-status s-action sc-bytes cs-bytes cs-method cs-uri-scheme cs-host cs-uri-path cs-uri-query cs-username s-hierarchy s-supplier-name rs(Content-Type) cs(User-Agent) sc-filter-result sc-filter-category x-virus-id s-ip s-sitename x-virus-details x-icap-error-code x-icap-error-details\r\n2005-04-12 22:40:07 2 10.0.1.16 403 TCP_DENIED 775 456 GET http www.google.com / - - NONE - - \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; FunWebProducts-MyWay)\" DENIED none - 10.0.1.1 SG-HTTP-Service - - -\r\n2005-04-12 22:40:34 30 192.16.170.46 302 TCP_NC_MISS 547 581 GET http us.rd.yahoo.com /travel/fp/qs/sb/hotel/*http://travel.yahoo.com/ ?qs=h - DIRECT us.rd.yahoo.com text/html \"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.7.6) Gecko/20050317 Firefox/1.0.2\" PROXIED none - 192.16.170.42 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:40:35 468 192.16.170.46 200 TCP_NC_MISS 10754 720 GET http ad.doubleclick.net /adi/N447.travelocity.yahoo/B1498841.21;sz=250x250;dcopt=rcl;click=http://us.ard.yahoo.com/SIG=127421do5/M=325506.6272882.7262842.4116762/D=travel/S=96146379:TS/_ylt=AvN7WyZm.ROCR8YJnxBUvXnLE7sF/EXP=1113352834/A=2519747/R=0/*;ord=1113345634216813 ? - DIRECT ad.doubleclick.net text/html \"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.7.6) Gecko/20050317 Firefox/1.0.2\" PROXIED none - 192.16.170.42 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:40:36 654 192.16.170.46 200 TCP_MISS 17869 674 GET http m3.doubleclick.net /551711/250x250_30k_Backup_March22.gif - - DIRECT m3.doubleclick.net image/gif \"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.7.6) Gecko/20050317 Firefox/1.0.2\" PROXIED none - 192.16.170.42 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:40:39 57 192.16.170.46 302 TCP_NC_MISS 547 584 GET http us.rd.yahoo.com /travel/fp/qs/sb/car/*http://travel.yahoo.com/ ?qs=c - DIRECT us.rd.yahoo.com text/html \"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.7.6) Gecko/20050317 Firefox/1.0.2\" PROXIED none - 192.16.170.42 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n2005-04-12 22:40:40 194 192.16.170.46 200 TCP_NC_MISS 852 720 GET http view.atdmt.com /HBR/iview/yhxxxhrz0260000072hbr/direct/01/&time=1113345638722854 ?click=http://us.ard.yahoo.com/SIG=127qc9af8/M=325106.5675593.6872823.3812585/D=travel/S=96146380:TS/_ylt=AvN7WyZm.ROCR8YJnxBUvXnME7sF/EXP=1113352838/A=2514397/R=0/* - DIRECT view.atdmt.com text/html \"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.7.6) Gecko/20050317 Firefox/1.0.2\" PROXIED none - 192.16.170.42 SG-HTTP-Service - server_unavailable \"Server unavailable: No ICAP server is available to process request.\"\r\n";
     Reader targetReader = new StringReader(input);
     //LineNumberReader lineNumberReader = new LineNumberReader(targetReader);
     System.out.println ("===========printnasdasdasd=============");

     System.out.println (targetReader.toString());
     System.out.println ("===========working=============");
     ElfParser parser = ElfParserBuilder.of()
                .build(targetReader);
        // Copy the messages to our outputLogEntry entry;
        //Map<String, Object> entry;
        LogEntry entry;

        while (null != (entry = parser.next())) {
            messages.add(entry);
            //entry = entry0.fieldData();
            //other.add(entry);
            System.out.println(messages);

        }
        System.out.println(messages);
    /**
     * Parse ELFF Stream Constructor.
     */
    //private ElffParser() {
    //}

        System.out.println(other);

    }

}
