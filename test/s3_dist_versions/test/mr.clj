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

(ns s3-dist-versions.test.mr
  (:require [clojure.test :refer :all]
            [amazonica.aws.s3 :as aws-s3]
            [cheshire.core :as json]
            [clj-time.core :as ct]
            [clj-time.format :as ctf]
            [clojure.java.io :as jio]
            [clojure.string :as string]
            [clojure-hadoop.job :as job]
            [hdfs.core :as hdfs]
            [plumbing.core :refer [map-from-vals map-vals]]
            [s3-dist-versions.mr :as mr]
            [s3-dist-versions.s3 :as s3]
            [s3-dist-versions.utils :refer [with-temp-dir]]
            [s3-dist-versions.test.helpers :refer [mktime]])
  (:import [org.apache.hadoop.io BytesWritable LongWritable Text]))

(defn- text-contents
  [s]
  (->> s
       (map #(str % "\n"))
       (string/join "")))

(deftest test-shuffle-mr
  (testing "shuffle-mr shuffles"
    (with-temp-dir [temp-dir]
      (let [input (map str (range 1000))
            _ (spit (str temp-dir "/input") (text-contents input))
            job (mr/shuffle-mr "test"
                               (str temp-dir "/input")
                               (str temp-dir "/output"))]
        (is (.isSuccessful (job/run job)))
        (let [output (->> "/output/part-r-00000"
                          (str temp-dir)
                          hdfs/sequence-file-seq
                          (map #(apply mr/nippy-reader %))
                          (map second))]
          (is (= (set input) (set output)))
          (is (not= input output)))))))

(deftest test-prefix-input
  (testing "prefix-input points to file"
    (is (= (mr/prefix-input "x" :temp) "x"))
    (is (= (mr/prefix-input "a/b/c" :temp) "a/b/c")))
  (testing "prefix-input defaults to [\"\"]"
    (with-temp-dir [temp-dir]
      (is (re-matches #".*s3-dist-versions-empty-prefixes$"
                      (mr/prefix-input nil temp-dir))))))

(defn entry
  [key- t & [delete-marker? is-current?]]
  (cond-> {:key key-
           :bucket-name "BUCKET"
           :version-id t
           :last-modified (mktime t)
           :delete-marker? (boolean delete-marker?)}
    (not (nil? is-current?)) (assoc :is-current is-current?)))

(def prefix-data
  {"prefix1" (concat
               (for [t (range 10)]
                 (entry "prefix1/a" t))
               (for [t (range 5 200)]
                 (entry "prefix1--x" t))
               [(entry "prefix1/de/le/ted" 5)
                (entry "prefix1/de/le/ted" 10 true)])
   "prefix2" [(entry "prefix2/x" 0)
              (entry "prefix2/y/z" 100)]})

(def prefixes (keys prefix-data))

(def versions
  (->> prefix-data
       (mapcat second)
       (group-by :key)
       (map (fn [[k v]] [k (json/generate-string v)]))
       (into {})))

(def restore-time (mktime 25))

(def selected-versions
  (map-from-vals
    :key
    [(entry "prefix1/a" 9 false true)
     (entry "prefix1--x" 25 false false)
     (entry "prefix1/de/le/ted" 10 true true)
     (entry "prefix2/x" 0 false true)
     (entry "prefix2/y/z" nil true false)]))

(defn read-tsv
  [fname]
  (as-> fname $
    (slurp $)
    (string/split $ #"\n")
    (map #(string/split % #"\t") $)))

(deftest test-get-version-mr
  (with-temp-dir [temp-dir]
    (let [_ (spit (str temp-dir "/prefixes") (text-contents prefixes))
          shuffle-job (mr/shuffle-mr "test"
                                     (str temp-dir "/prefixes")
                                     (str temp-dir "/shuffled-prefixes"))
          _ (is (.isSuccessful (job/run shuffle-job)))
          _ (with-redefs  [s3/versions  (fn  [_ prefix]  (prefix-data prefix))]
              (is (.isSuccessful
                    (job/run (mr/get-version-mr
                               {:src-bucket "BUCKET"
                                :src-prefix ""
                                :restore-time restore-time
                                :dest-bucket "BUCKET"
                                :dest-prefix ""
                                :delete true}
                               (str temp-dir "/shuffled-prefixes")
                               (str temp-dir "/output"))))))
          output (read-tsv (str temp-dir "/output/part-r-00000"))]
      (is (= selected-versions (->> output
                                    (into {})
                                    (map-vals #(json/parse-string % true))
                                    (map-vals #(update-in % [:last-modified]
                                                          ctf/parse))))))))

(defn expected-restore
  "Make a function that gives the expected restoration outcome of a version."
  [dest-bucket dest-prefix delete]
  (fn [{:keys [bucket-name key version-id last-modified delete-marker?]
        :as version}]
    (cond
      ;; no copy
      (and (= "BUCKET" dest-bucket)
           (= "" dest-prefix)
           (get-in selected-versions [key :is-current]))
      nil

      ;; delete
      (or delete-marker?
          (> version-id 25))
      (if delete
        {:delete true
         :bucket-name dest-bucket
         :key (str dest-prefix key)}
        nil)

      ;; copy
      :else
      {:source-bucket-name bucket-name
       :destination-bucket-name dest-bucket
       :source-key key
       :destination-key (str dest-prefix key)
       :source-version-id version-id})))

(deftest test-restore-version-mr
  (doseq [delete [false true]
          dest-bucket ["BUCKET" "DEST-BUCKET"]
          dest-prefix ["" "dest-prefix/"]]
    (testing
      (format "restore-version-mr delete=%s dest-bucket=%s dest-prefix=%s"
              delete dest-bucket dest-prefix)
      (with-temp-dir [temp-dir]
        (with-redefs [aws-s3/delete-object (fn [bucket key-]
                                             {:delete true
                                              :bucket-name bucket
                                              :key key-})
                      aws-s3/copy-object identity]
          (spit (str temp-dir "/versions")
                (->> selected-versions
                     (map-vals json/generate-string)
                     (map #(string/join "\t" %))
                     text-contents))
          (is (.isSuccessful
                (job/run (mr/shuffle-mr "test"
                                        (str temp-dir "/versions")
                                        (str temp-dir "/shuffled-versions")))))
          (is (.isSuccessful
                (job/run (mr/restore-version-mr
                           {:src-bucket "BUCKET"
                            :src-prefix ""
                            :restore-time restore-time
                            :dest-bucket dest-bucket
                            :dest-prefix dest-prefix
                            :delete delete}
                           (str temp-dir "/shuffled-versions")
                           (str temp-dir "/output")))))
          (is (= (->> selected-versions
                      (map-vals (expected-restore
                                  dest-bucket dest-prefix delete))
                      ;; Serialize and deserialize to match stringification
                      ;; of MR
                      (map-vals json/generate-string)
                      (map-vals json/parse-string))
                 (->> (str temp-dir "/output/part-r-00000")
                      read-tsv
                      (into {})
                      (map-vals json/parse-string)))))))))
