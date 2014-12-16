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

(ns s3-dist-versions.main
  "Main functions for s3-dist-versions."
  (:require [clj-time.core :as ct]
            [clj-time.format :as ctf]
            [amazonica.aws.s3 :as aws-s3]
            [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [plumbing.map :refer [keyword-map]]
            [s3-dist-versions.mr :as mr]
            [s3-dist-versions.s3 :as s3]
            [s3-dist-versions.utils :as utils])
  (:gen-class))


(def cli-options
  [[nil "--src LOCATION"
    "S3 location from which to read versioned data"]
   [nil "--prefixes PREFIXES"
    "S3 key prefixes in the src to restore; parallelism is partially obtained by listing prefixes in parallel"
    :default nil]
   [nil "--restore-time TIME"
    "For each object, restore to the last version before or equal to TIME (format ISO 8601)"
    :parse-fn ctf/parse]
   [nil "--dest LOCATION"
    "S3 destination for restored data. If not given, overwrite source."
    :default nil]
   [nil "--version-info-output OUTPUT_DIRECTORY"
    "Directory for MR version information output"]
   [nil "--delete"
    "If delete is set, delete files that were not present at restore-time. Default: false."
    :default false]
   [nil "--help"]])

(defn arg-validation
  "Check arguments and return a list of (string) validation errors."
  [{:keys [src prefixes restore-time dest version-info-output delete help]}]
  (for [[check msg] [[(instance? org.joda.time.DateTime restore-time)
                      "Must specify --restore-time in ISO-8601 format"]
                     [(and src (s3/s3-uri? src))
                      (format "Must specify --src S3 path; got %s" src)]
                     [(or (nil? dest) (s3/s3-uri? dest))
                      "If --dest is given, it must be an S3 path"]]
        :when (not check)]
    msg))

(defn- run-mr
  "Take the sanitized, correct options and actually run the restore flow."
  [tool {:keys [src dest prefixes restore-time version-info-output delete]
         :as options}]
  (let [version-output (when version-info-output
                         (str version-info-output "/versions"))
        restored-output (when version-info-output
                          (str version-info-output "/restored"))
        dest* (or dest src)
        [src-bucket src-prefix] (s3/parse-s3-uri src)
        [dest-bucket dest-prefix] (s3/parse-s3-uri dest*)
        ;; The prefix input should be treated as files, so it has to be an
        ;; s3n:// URL, but the user shouldn't have to know that.
        prefixes (when prefixes (string/replace prefixes "^s3://" "s3n://"))]
    (println
      (format (str "Restoring data from %s (prefix file %s) "
                   "at time %s to destination %s")
              src prefixes restore-time dest*))
    (mr/restore-flow
      tool
      (keyword-map src-bucket src-prefix dest-bucket dest-prefix
                   restore-time delete)
      prefixes version-output restored-output)))

(defn configured-main
  "Parse arguments and run the version restoration mapreduce flow."
  [tool args]
  (let [{:keys [options errors summary]} (cli/parse-opts args cli-options)
        validation-errors (concat errors (arg-validation options))]
    (cond
      (:help options) (do (println (str "usage: \n" summary))
                          2)  ; Return 2: an error code
      (seq validation-errors) (do (doseq [err validation-errors]
                                    (binding [*out* *err*] (println err)))
                                  2)  ; Return 2: an error code
      :else (if (run-mr tool options)
              0  ; Return 0: success
              1  ; Return 1: error
              ))))

(defn run
  "Use a Hadoop Tool to parse Hadoop options and run the mapreduce flow."
  [& args]
  (utils/tool-run configured-main args))

(defn -main
  [& args]
  (System/exit (apply run args)))
