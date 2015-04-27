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

/**
 * Filter for deciding, whether a particular nuth-crawled content should be exported to the
 * WARC file in {@link net.sf.nutchcontentexporter.NutchToWARCConverter}
 *
 * @author Ivan Habernal
 */
public interface ExportContentFilter
{
    /**
     * Returns true, if the filter should accept this content for exporting to WARC file; false
     * otherwise
     *
     * @param recordInfo record info
     * @return boolean value
     */
    boolean acceptContent(WARCRecordInfo recordInfo);
}
