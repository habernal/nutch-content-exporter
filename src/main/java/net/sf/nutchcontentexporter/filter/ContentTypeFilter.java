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

import org.archive.io.warc.WARCRecordInfo;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Ivan Habernal
 */
public class ContentTypeFilter
        implements ExportContentFilter
{

    /**
     * We accept only html
     * In a text run, the distribution was text/xml=13%, text/html=83%, everything else<=1%
     */
    private static final Set<String> ACCEPTED_CONTENT_TYPE = new HashSet<String>(
            Arrays.asList("text/xml", "text/html"));

    /**
     * Returns the given content-type or empty string, if not available
     *
     * @param recordInfo WARC record info
     * @return string, never null
     */
    protected String getContentType(WARCRecordInfo recordInfo)
    {
        String contentTypeFull = recordInfo.getExtraHeaders().asMap().get("Nutch_Content-Type");

        if (contentTypeFull != null) {
            // split by ";"
            return contentTypeFull.split(";")[0];
        }

        return "";
    }

    @Override
    public boolean acceptContent(WARCRecordInfo recordInfo)
    {
        String contentType = getContentType(recordInfo);

        return ACCEPTED_CONTENT_TYPE.contains(contentType);
    }
}
