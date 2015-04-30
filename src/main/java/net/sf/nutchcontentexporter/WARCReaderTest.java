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

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author Ivan Habernal
 */
public class WARCReaderTest
{
    public static void main(String[] args)
            throws Exception
    {
        read(args[0]);
    }

    /**
     * Reads default (gzipped) warc file
     *
     * @param file gz file
     * @throws IOException
     */
    public static void read(String file)
            throws IOException
    {
        WARCReader reader = WARCReaderFactory.get(new File(file));

        int counter = 0;
        for (ArchiveRecord record : reader) {
            System.out.println(record.getHeader().getHeaderFields());

            counter++;
        }

        System.out.println(counter);
    }

    /**
     * Reads bz2 warc file
     *
     * @param file warc file
     * @throws IOException
     */
    public static void readBz2(String file)
            throws IOException
    {
        // decompress bz2 file to tmp file
        File tmpFile = File.createTempFile("tmp", ".warc");
        BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(
                new FileInputStream(file));

        IOUtils.copy(inputStream, new FileOutputStream(tmpFile));

        WARCReader reader = WARCReaderFactory.get(tmpFile);

        int counter = 0;
        for (ArchiveRecord record : reader) {
            System.out.println(record.getHeader().getHeaderFields());

            counter++;
        }

        FileUtils.forceDelete(tmpFile);

        System.out.println(counter);
    }
}
