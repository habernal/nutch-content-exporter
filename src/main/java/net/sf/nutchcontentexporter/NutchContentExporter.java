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

import java.io.File;
import java.io.FileOutputStream;

/**
 * Simple command line tool for exporting HTML pages crawled by Apache Nutch
 */
public class NutchContentExporter
        extends Configured
        implements Tool
{

    private static final int MAX_FILE_NAME_LENGTH = 255;

    public static void main(String[] args)
            throws Exception
    {
        ToolRunner.run(new NutchContentExporter(), args);
    }

    @Override
    public int run(String[] args)
            throws Exception
    {
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.out.println(
                    "two params required: segmentdir (-local | -dfs <namenode:port>) outputdir");
            return 1;
        }

        try {
            FileSystem fs = FileSystem.get(conf);

            String segment = otherArgs[0];

            File outDir = new File(otherArgs[1]);
            if (!outDir.exists()) {
                if (outDir.mkdirs()) {
                    System.out.println("Creating output dir " + outDir.getAbsolutePath());
                }
            }

            Path file = new Path(segment, Content.DIR_NAME + "/part-00000/data");
            // new 2.0 API
            SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(file));

            Text key = new Text();
            Content content = new Content();

            while (reader.next(key, content)) {
                String filename =
                        key.toString().replaceAll("^http[s]?://", "").replaceAll("/$", "").replaceAll("/", "___").trim();

                // limit the output file name to 255 characters
                if (filename.length() > MAX_FILE_NAME_LENGTH) {
                    filename = filename.substring(0, MAX_FILE_NAME_LENGTH);
                }


                File f = new File(outDir.getCanonicalPath() + "/" + filename);
                FileOutputStream fos = new FileOutputStream(f);
                fos.write(content.getContent());
                fos.close();
                System.out.println(f.getAbsolutePath());
            }
            reader.close();
            fs.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        return 0;
    }
}