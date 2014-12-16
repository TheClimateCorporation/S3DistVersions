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

(ns s3-dist-versions.mr
  "MapReduce jobs and flows.

  This library contains three main MapReduce jobs (reordering data, listing
  versions, and restoring versions), and one flow (restore-flow) that calls all
  those jobs."
  (:require [cheshire.core :as json]
            [cheshire.generate]
            [clj-time.core :as ct]
            [clj-time.format :as ctf]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure-hadoop.config :as config]
            [clojure-hadoop.job :as job]
            [hdfs.core :as hdfs]
            [s3-dist-versions.s3 :as s3]
            [taoensso.nippy :as nippy])
  (:import [org.apache.hadoop.io BytesWritable IntWritable Text]
           [org.apache.hadoop.mapreduce JobContext TaskInputOutputContext]))


;; Add JSON encoding for DateTimes.
(cheshire.generate/add-encoder
  org.joda.time.DateTime
  (fn [dt jsonGenerator]
    (.writeString jsonGenerator (str (ct/to-time-zone dt ct/utc)))))

(defn- freeze-writable
  "Serialize data to a BytesWritable."
  ^BytesWritable [x]
  (-> x
      (nippy/freeze {:compressor nil})
      BytesWritable.))

(defn- thaw-writable
  "Deserialize data from a BytesWritable."
  [^BytesWritable x]
  (-> x .getBytes nippy/thaw))

(defn nippy-writer
  "A map or reduce writer that encodes data with nippy."
  [^TaskInputOutputContext context k v]
  (.write context
          (freeze-writable k)
          (freeze-writable v)))

(defn nippy-reader
  "A map or reduce reader that decodes data with nippy."
  [^BytesWritable wkey wvalue]
  [(thaw-writable wkey)
   (if (instance? org.apache.hadoop.mapreduce.ReduceContext$ValueIterable
                  wvalue)
     (map thaw-writable (iterator-seq (.iterator wvalue)))
     (thaw-writable wvalue))])

(defn string-string-writer
  "A map or reduce writer for String String output."
  [^TaskInputOutputContext context ^String k ^String v]
  (.write context (Text. k) (Text. v)))

(defn string-string-reader
  "A map or reduce writer for String String input."
  [^Text wkey wvalue]
  [(.toString wkey)
   (if (instance? org.apache.hadoop.mapreduce.ReduceContext$ValueIterable
                  wvalue)
     (map str (iterator-seq (.iterator wvalue)))
     (str wvalue))])

(defn- add-hash-key
  "Map [k v] to [hash-key, [k, v]]"
  [k v]
  (let [serial (nippy/freeze [k v] {:compressor nil})]
    [[(hash serial) serial]]))

(defn- remove-hash-key
  "Reduce [hash-key [[k v]]] to [k v]"
  [h vs]
  (map nippy/thaw vs))

(defn- choose-num-reduces
  "A function to configure a job to use a given number of reduces per map slot.
  This is useful to give more parallelism when accessing S3."
  [reduces-per-map-slot]
  (fn [job]
    (let [config (config/configuration job)
          map-slots (-> config
                        org.apache.hadoop.mapred.JobConf.
                        org.apache.hadoop.mapred.JobClient.
                        .getClusterStatus
                        .getMaxMapTasks)]
      (.setNumReduceTasks config (int (* map-slots reduces-per-map-slot))))))

(defn shuffle-mr
  "Mapreduce: Given a text file, output a seqfile with data shuffled by the
  hash of the data, effectively randomizing its order."
  [name- input output]
  {:name name-
   :input input
   :input-format :text
   :map-reader "clojure-hadoop.wrap/int-string-map-reader"
   :map "s3-dist-versions.mr/add-hash-key"
   :map-writer "s3-dist-versions.mr/nippy-writer"
   :map-output-key "org.apache.hadoop.io.BytesWritable"
   :map-output-value "org.apache.hadoop.io.BytesWritable"
   :reduce-reader "s3-dist-versions.mr/nippy-reader"
   :reduce "s3-dist-versions.mr/remove-hash-key"
   :reduce-writer "s3-dist-versions.mr/nippy-writer"
   :output output
   :output-key "org.apache.hadoop.io.BytesWritable"
   :output-value "org.apache.hadoop.io.BytesWritable"
   :output-format :seq
   :compress-output false
   :configure [;; We want lots of map tasks in the S3-accessing steps The
               ;; input work looks small, but we want parallelism accessing S3
               ;; here. A bit less than 4 tasks per mapper (less so there's
               ;; some capacity to handle any failures) sounds good.
               (choose-num-reduces 3.5)]})

(defn prefix-input
  "Default nil prefix input file to a single empty prefix."
  [prefix-file prefix-temp]
  (if prefix-file
    prefix-file
    ;; Create an empty input file
    (doto (str prefix-temp "/s3-dist-versions-empty-prefixes")
      (hdfs/spit "\n"))))

(def ^:private restore-info-param
  "A mapreduce configuration key for storing restore-info."
  "s3-dist-versions.restore-info")

;; Unfortunately, clojure-hadoop uses map and reduce setup functions, so we
;; need this global mutable variable to do that setup.
(let [restore-info (atom {:src-bucket nil
                          :src-prefix nil
                          :dest-bucket nil
                          :dest-prefix nil
                          :delete nil
                          :restore-time nil})]

  (defn- read-restore-info!
    "Read the restore-info configuration into the atom."
    [^JobContext context]
    (reset! restore-info
            (-> context
                .getConfiguration
                (.get restore-info-param)
                read-string
                (update-in [:restore-time] ctf/parse))))

  (defn- restore-info-encoder
    "Make a function to serialize the given restore-info into a mapreduce
    configuration."
    [info]
    (let [encoded (-> info
                      ;; We have to be a little careful with serialization of
                      ;; datetimes.
                      (update-in [:restore-time] str)
                      pr-str)]
      (fn [job]
        (.set (config/configuration job) restore-info-param encoded))))

  (defn version-mapper
    "Given a source and a collection of S3 key prefixes under that source,
    produce a collection of versions under each prefix.

    The version information is encoded as JSON for potential compatibility with
    other tools."
    [_ s3-partial-key]
    (->> s3-partial-key
         (str (:src-prefix @restore-info))
         (s3/versions (:src-bucket @restore-info))
         (map (fn [version-info]
                [(:key version-info) (json/generate-string version-info)]))))

  (defn- select-version
    "Given a list of versions, find the latest version before the restore time."
    [versions restore-time]
    (->> versions
         (remove #(ct/after? (:last-modified %) restore-time))
         last))

  (defn select
    "Select the target version from some sorted versions or make a delete
    version."
    [key- {:keys [restore-time src-bucket] :as restore-info} versions]
    (let [current-version (last versions)
          target-version (or (select-version versions restore-time)
                             {:key key- :version-id nil :delete-marker? true
                              :bucket-name src-bucket :last-modified false})
          annotated-version (assoc target-version
                                   :is-current (= target-version
                                                  current-version))]
      annotated-version))

  (defn restore
    "Restore a version."
    [target-version key-
     {:keys [src-bucket src-prefix dest-bucket dest-prefix delete]
      :as restore-info}]
    (let [copying-in-place? (= [src-bucket src-prefix] [dest-bucket dest-prefix])]
      (if (and copying-in-place? (:is-current target-version))
        (log/infof "Not recopying the current version of %s %s"
                   key- target-version)
        (s3/restore-version target-version src-bucket src-prefix
                            dest-bucket dest-prefix delete))))

  (defn version-reducer
    "Reduce [S3-key, [JSON-version-information]] to
    [S3-key, JSON-target-version-information]."
    [s3-key version-info-f]
    [[s3-key (->> version-info-f
                  (map #(json/parse-string % true))
                  (map #(update-in % [:last-modified] ctf/parse))
                  (sort-by :last-modified)
                  (select s3-key @restore-info)
                  json/generate-string)]])

  (defn restore-version-mapper
    "Given a text line containing [S3-key JSON-target-verison-information],
    restore that version."
    [_ text-line]
    (let [[key- version-json] (string/split text-line #"\t")]
      [[key- (-> version-json
                 (json/parse-string true)
                 (update-in [:last-modified] ctf/parse)
                 (restore key- @restore-info)
                 json/generate-string)]])))

(defn get-version-mr
  "Mapreduce: Given a seqfile of S3 key prefixes, make an text file of
  [S3-key, JSON-[version-information]]."
  [restore-info input output]
  {:name "list-versions"
   :input input
   :input-format :seq
   :map-setup "s3-dist-versions.mr/read-restore-info!"
   :map-reader "s3-dist-versions.mr/nippy-reader"
   :map "s3-dist-versions.mr/version-mapper"
   :map-writer "s3-dist-versions.mr/nippy-writer"
   :map-output-key "org.apache.hadoop.io.BytesWritable"
   :map-output-value "org.apache.hadoop.io.BytesWritable"
   :reduce-reader "s3-dist-versions.mr/nippy-reader"
   :reduce "s3-dist-versions.mr/version-reducer"
   :reduce-writer "s3-dist-versions.mr/string-string-writer"
   :output output
   :output-format :text
   :compress-output false
   :configure [;; We want 1 reduce per map slot so we don't lose too much
               ;; parallelism.
               (choose-num-reduces 1)
               (restore-info-encoder restore-info)]})

(defn first-reducer
  "A reducer that returns the first value."
  [k vs]
  [[k (first vs)]])

(defn restore-version-mr
  "Mapreduce: Given a seqfile of S3 keys and their versions, figure out what
  version was present at the target time and restore it. Emit the results as a
  text file."
  [restore-info input output]
  {:name "restore-versions"
   :input input
   :input-format :seq
   :map-setup "s3-dist-versions.mr/read-restore-info!"
   :map-reader "s3-dist-versions.mr/nippy-reader"
   :map "s3-dist-versions.mr/restore-version-mapper"
   :map-writer "s3-dist-versions.mr/string-string-writer"
   :map-output-key "org.apache.hadoop.io.Text"
   :map-output-value "org.apache.hadoop.io.Text"
   :reduce-reader "s3-dist-versions.mr/string-string-reader"
   :reduce "s3-dist-versions.mr/first-reducer"
   :reduce-writer "s3-dist-versions.mr/string-string-writer"
   :output output
   :output-format :text
   :compress-output false
   :configure [(restore-info-encoder restore-info)]})

(defn restore-flow
  "The entire restoration mapreduce flow."
  [tool restore-info prefixes version-output restored-output]
  (hdfs/with-fs-tmp
    [fs
     ;; Places to put intermediates
     prefix-temp prefix-shuffled version-shuffled
     ;; Places to put version output if it wasn't specified
     local-version-output local-restored-output]
    (let [input (prefix-input prefixes prefix-temp)
          version-output (or version-output local-version-output)
          restored-output (or restored-output local-restored-output)]
      (every? (fn [job]
                (println "Running step:" (:name job))
                (.isSuccessful (job/run tool job)))
              [(shuffle-mr "reorder-prefixes" input prefix-shuffled)
               (get-version-mr restore-info prefix-shuffled version-output)
               (shuffle-mr "reorder-s3-keys" version-output version-shuffled)
               (restore-version-mr
                 restore-info version-shuffled restored-output)]))))
