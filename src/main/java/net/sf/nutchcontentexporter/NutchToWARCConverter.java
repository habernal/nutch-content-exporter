/*
 * Copyright 2015 Ivan Habernal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.sf.nutchcontentexporter;

import net.sf.nutchcontentexporter.filter.ContentTypeFilter;
import net.sf.nutchcontentexporter.filter.ExportContentFilter;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.protocol.Content;
import org.archive.format.warc.WARCConstants;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.io.warc.WARCWriter;
import org.archive.io.warc.WARCWriterPoolSettingsData;
import org.archive.uid.RecordIDGenerator;
import org.archive.uid.UUIDGenerator;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Converts a content from Nutch stored in a segment folder into a compressed WARC file
 *
 * @author Ivan Habernal
 */
public class NutchToWARCConverter
        extends Configured
        implements Tool
{
    // Thu, 01 Jan 1970 00:00:01 GMT
    private static final String DEFAULT_WARC_DATE = "1000";

    protected RecordIDGenerator generator = new UUIDGenerator();

    /**
     * Filters for deciding whether a particular crawled document should be exported to the
     * final WARC file
     */
    private final Set<ExportContentFilter> filters = new HashSet<ExportContentFilter>();

    /**
     * Add the filters to the filter set
     *
     * @param filters filters
     */
    public void addFilters(ExportContentFilter... filters)
    {
        this.filters.addAll(Arrays.asList(filters));
    }

    /**
     * Converts a content from Nutch stored in a segment folder into a bz2 WARC file
     *
     * @param segmentFile Nutch segment folder
     * @param warc        output warc file
     * @param conf        hadoop configuraion
     * @param compressBz2
     * @throws IOException
     * @throws ParseException
     */
    public void nutchSegmentToWARCFile(Path segmentFile, File warc,
            Configuration conf, boolean compressBz2)
            throws IOException, ParseException
    {
        // reader for hadoop sequence file
        SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(segmentFile));

        // create a warc writer
        OutputStream outputStream;

        if (compressBz2) {
            // we don't compress using the built-in GZ support, use bz2 instead
            outputStream = new BZip2CompressorOutputStream(
                    new BufferedOutputStream(new FileOutputStream(warc)));
        }
        else {
            // default compression (gz)
            outputStream = new FileOutputStream(warc);
        }
        WARCWriter writer = new WARCWriter(new AtomicInteger(), outputStream, warc,
                new WARCWriterPoolSettingsData("", "", -1, !compressBz2, null, null, generator));

        // warcinfo record
        writer.writeWarcinfoRecord(warc.getName(),
                "Made by " + this.getClass().getName() + "/" + getRevision());

        Text key = new Text();
        Content content = new Content();

        while (reader.next(key, content)) {
            write(writer, content);
        }

        writer.close();
        reader.close();
    }

    private static String getRevision()
    {
        return "1";
    }

    protected void write(final WARCWriter writer, final Content content)
            throws IOException, ParseException
    {
        WARCRecordInfo recordInfo = new WARCRecordInfo();
        recordInfo.setUrl(content.getUrl());

        byte[] byteContent = content.getContent();

        // skip empty records
        if (byteContent.length == 0) {
            return;
        }

        recordInfo.setContentStream(new ByteArrayInputStream(byteContent));
        recordInfo.setContentLength(byteContent.length);
        recordInfo.setEnforceLength(true);

        String warcDateString = DEFAULT_WARC_DATE;

        // convert date to WARC-Date format
        String date = content.getMetadata().get("Date");
        if (date != null) {
            try {
                warcDateString = String.valueOf(
                        new SimpleDateFormat("EEE, dd MMM yyyy kk:mm:ss ZZZ", Locale.ENGLISH)
                                .parse(date).getTime());
            }
            catch (ParseException ex) {
                // ignore
            }
        }

        recordInfo.setCreate14DigitDate(warcDateString);

        recordInfo.setType(WARCConstants.WARCRecordType.response);
        recordInfo.setMimetype(WARCConstants.HTTP_RESPONSE_MIMETYPE);
        recordInfo.setRecordId(generator.getRecordID());

        // add some extra headers from nutch
        Set<String> extraHeaders = new HashSet<String>(Arrays.asList("nutch.crawl.score",
                "nutch.segment.name", "Set-Cookie", "Content-Type", "Server", "Pragma",
                "Cache-Control"));

        for (String extraHeader : extraHeaders) {
            String value = content.getMetadata().get(extraHeader);
            if (value != null) {
                recordInfo.addExtraHeader("Nutch_" + extraHeader, value);
            }
        }

        // apply filters
        boolean acceptExport = true;
        for (ExportContentFilter filter : filters) {
            acceptExport &= filter.acceptContent(recordInfo);
        }

        // and write only if we accept this content
        if (acceptExport) {
            writer.writeRecord(recordInfo);
        }
    }

    /**
     * Input: Nutch segment folder (e.g. "20150303005802")
     * Ouput: gz/bz2 WARC file (e.g. "20150303005802.warc.gz/bz2")
     * Third parameter is an ouput file prefix (e.g. "prefix20150303005802.warc.gz")
     * <p/>
     * By default, the output is compressed with gz
     *
     * @param args args
     * @return int
     * @throws Exception
     */
    @Override
    public int run(String[] args)
            throws Exception
    {
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        try {
            FileSystem fs = FileSystem.get(conf);

            String segmentDir = otherArgs[0];

            File outDir = new File(otherArgs[1]);
            if (!outDir.exists()) {
                if (outDir.mkdirs()) {
                    System.out.println("Creating output dir " + outDir.getAbsolutePath());
                }
            }

            String outputFilePrefix = "";
            if (otherArgs.length >= 3) {
                outputFilePrefix = otherArgs[2];
            }

            boolean compressBz2 = false;
            // do we want bz2 output?
            if (otherArgs.length >= 4) {
                compressBz2 = "bz2".equals(otherArgs[3]);
            }

            Path file = new Path(segmentDir, Content.DIR_NAME + "/part-00000/data");

            String extension = ".warc." + (compressBz2 ? "bz2" : "gz");

            String segmentName = new File(segmentDir).getName();
            nutchSegmentToWARCFile(file,
                    new File(outDir, outputFilePrefix + segmentName + extension), conf,
                    compressBz2);

            fs.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        return 0;
    }

    public static void main(String[] args)
    {
        try {
            NutchToWARCConverter nutchToWARCConverter = new NutchToWARCConverter();
            nutchToWARCConverter.addFilters(new ContentTypeFilter());
            ToolRunner.run(nutchToWARCConverter, args);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
