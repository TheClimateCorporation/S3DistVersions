;; The Climate Corporation licenses this file to you under under the Apache
;; License, Version 2.0 (the "License"); you may not use this file except in
;; compliance with the License.  You may obtain a copy of the License at
;;
;;   http://www.apache.org/licenses/LICENSE-2.0
;;
;; See the NOTICE file distributed with this work for additional information
;; regarding copyright ownership.  Unless required by applicable law or agreed
;; to in writing, software distributed under the License is distributed on an
;; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
;; or implied.  See the License for the specific language governing permissions
;; and limitations under the License.

(defproject s3-dist-versions "0.1.0"
  :description "Mapreduce to restore S3 versioned files"
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [amazonica "0.2.29" :exclusions [org.apache.httpcomponents/httpclient]]
                 [cheshire "5.3.1"]
                 [clj-time "0.8.0"]
                 [clojure-hadoop "1.4.4" :exclusions [log4j]]
                 [com.taoensso/nippy "2.7.0"]
                 [hdfs-clj "0.1.13"]
                 [org.apache.httpcomponents/httpclient "4.2.5"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [prismatic/plumbing "0.3.5"]]
  :jar-name "S3DistVersions-nonuber-%s.jar"
  :uberjar-name "S3DistVersions-%s.jar"
  :profiles {:dev {:dependencies [[commons-io "2.4"]]}}
  :aot [s3-dist-versions.main]
  :main s3-dist-versions.main)
