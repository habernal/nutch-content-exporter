/*
 * Copyright 2017 Ivan Habernal
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


import org.apache.commons.math3.stat.Frequency;
import org.archive.io.warc.WARCRecordInfo;

/**
 * Always accepting the entry, collecting statistics only
 *
 * @author Ivan Habernal
 */
public class ContentTypeStatistics
        extends ContentTypeFilter
{
    private Frequency frequency = new Frequency();

    @Override
    public boolean acceptContent(WARCRecordInfo recordInfo)
    {
        frequency.addValue(getContentType(recordInfo));

        return true;
    }

    /**
     * Content-type frequency
     *
     * @return frequency
     */
    public Frequency getFrequency()
    {
        return frequency;
    }
}
