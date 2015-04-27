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

package net.sf.nutchcontentexporter.filter;

import org.apache.commons.io.IOUtils;
import org.archive.io.warc.WARCRecordInfo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

/**
 * @author Ivan Habernal
 */
public class CreativeCommonsCandidateFilter
        implements ExportContentFilter
{
    @Override
    public boolean acceptContent(WARCRecordInfo recordInfo)
    {
        ByteArrayInputStream contentStream = (ByteArrayInputStream) recordInfo.getContentStream();

        try {
            List<String> lines = IOUtils.readLines(contentStream);
            contentStream.reset();

            for (String line : lines) {
                if (line.contains("creativecommons")) {
                    return true;
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return false;
    }
}
