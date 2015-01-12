Nutch Content Exporter - simple command line java program for exporting HTML pages crawled by
Apache Nutch to the file system.

Copyright (c) 2015 Ivan Habernal

Usage:
$mvn package
$java -jar target/nutchcontentexporter-1.0-SNAPSHOT.jar segment-dir output-dir

for example

$java -jar target/nutchcontentexporter-1.0-SNAPSHOT.jar /tmp/crawl/20150109134429/ /tmp/outhtml

where the input folder is the Nutch segment
$ tree /tmp/crawl/
/tmp/crawl/
└── 20150109134429
    ├── content
    │   └── part-00000
    │       ├── data
    │       └── index
    ├── crawl_fetch
    │   └── part-00000
    │       ├── data
    │       └── index
    ├── crawl_generate
    │   └── part-00000
    ├── crawl_parse
    │   └── part-00000
    ├── parse_data
    │   └── part-00000
    │       ├── data
    │       └── index
    └── parse_text
        └── part-00000
            ├── data
            └── index


The output files are stored under the original URL with all slashes ("/") replaced by three
underlines ("___"), e.g.

http://www.example.com/test.html -> www.example.com___test.html
http://www.example.com/test/ -> www.example.com___test___