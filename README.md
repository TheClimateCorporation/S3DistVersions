# S3DistVersions

Amazon's [Simple Storage Service (S3)][0] is a bulk key/value store service. If
you enable [versioning in S3][1], every time an object is updated, the old
version is not deleted, but instead a new version is written. Then, later, the
old version can be accessed or restored if needed. However, by default, S3 does
not provide an API for bulk version manipulation or restoration.

S3DistVersions is a tool to do bulk restore of versions in S3. Its design and
interface are loosely based on [S3DistCp][2], a tool for bulk copying of files in
S3. Just like S3DistCp, S3DistVersions is a Jar file which is meant to be run
in [Elastic MapReduce][3].

At its heart, S3DistVersions is designed to restore the contents of a bucket at
a particular time. It accesses a list of versions from S3, finds the one
version for each file that was present at that time, and restores (via copying)
that version.

[0]: http://aws.amazon.com/s3/
[1]: http://docs.aws.amazon.com/AmazonS3/latest/dev/Versioning.html
[2]: http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/UsingEMR_s3distcp.html
[3]: http://aws.amazon.com/elasticmapreduce/

# Download

You can access the latest jar at
[http://download.climate.com/S3DistVersions/releases/S3DistVersions-0.1.0.jar](http://download.climate.com/S3DistVersions/releases/S3DistVersions-0.1.0.jar).

# Usage

S3DistVersions takes the following arguments:

* --src LOCATION: Required. An S3 location containing the versioned data that
  will be restored.
* --restore-time TIME: Required. An [ISO-8601][4]-formatted time to restore to.
* --dest LOCATION: Optional. An S3 location where the versioned data will be
  restored to. It does not need to be versioned. If not given, dest defaults to
  src.
* --delete: Optional. If --delete is given, files that were not present in src
  at the restore-time will be deleted from dest. If --delete is not given,
  nothing will be deleted, though files may be overwritten.
* --version-info-output LOCATION: Optional. An S3 location that will receive
  information about the versions restored.
* --prefixes LOCATION: Optional. A text file of prefixes (one per line) under
  src that should be scanned for versions. This helps speed up listing of
  versions; see below under [Performance
  Considerations](#performance-considerations).

You can run S3DistVersions on EMR using [lemur][6]; we have provided an example
lemur configuration.

Alternatively, you can run S3DistVersions on EMR using [Amazon's AWS
command-line tool][7], for instance:

```shell
aws emr create-cluster --ami-version=2.5.9 \
  --instance-type=m1.small --instance-count=5 \
  --steps Type=CUSTOM_JAR,Name="S3DistVersions step",\
Jar=http://download.climate.com/S3DistVersions/releases/S3DistVersions-0.1.0.jar,\
Args=[\
"--src s3://mybucket/mypath",\
"--prefixes s3://mybucket/list-of-s3-prefixes.txt",\
"--restore-time 2014-01-01T14:00:00+07:00",\
"--dest s3://mybucket/newpath",\
"--version-info-output s3://mybucket/restored-versions"]
```

[4]: http://en.wikipedia.org/wiki/ISO_8601
[5]: http://xkcd.com/1179/
[6]: https://github.com/TheClimateCorporation/lemur
[7]: http://docs.aws.amazon.com/cli/latest/reference/emr/index.html

# Performance Considerations

Usually, the bottleneck in restoring old versions will be dealing with S3.
Listing versions is generally slow, but depending on your data size, restoring
the data can be, too. To deal with this, you usually want a lot of parallelism.

Unfortunately, version listing in S3 is generally a serial process. If
S3DistVersions doesn't know anything about the data you want to restore, it can
only list the versions in serial, because that is all the AWS API provides.
S3DistVersions also accepts a --prefixes option pointing to a text file with
one S3 prefix per line. If that is provided, those prefixes will be distributed
across mappers, providing parallelism.

Just to be clear about how to use prefixes, here's an example. Suppose you want
to copy `s3://mybucket/mypath`, which contains `s3://mybucket/mypath/1` and
`s3://mybucket/mypath/2`. Then you'd specify `--src s3://mybucket/mypath` and
include the following prefixes:

```
/1
/2
```

Note that it is still useful to follow the [usual recommendations for
distributing your key prefixes][8] in order to have S3 perform well.

You may also wish to use more mapper and reducer slots per node than you
usually would. The work done in this mapreduce is not particularly
intensive--the main work is done by S3--so you want as much parallelism as you
can get within the RAM constraints your cluster has.

[8]: http://docs.aws.amazon.com/AmazonS3/latest/dev/request-rate-perf-considerations.html

# License

Copyright (C) 2014 The Climate Corporation. Distributed under the Apache
License, Version 2.0. You may not use this library except in compliance with
the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

See the NOTICE file distributed with this work for additional information
regarding copyright ownership. Unless required by applicable law or agreed to
in writing, software distributed under the License is distributed on an "AS IS"
BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations
under the License.
