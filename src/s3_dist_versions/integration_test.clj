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

(ns s3-dist-versions.integration-test
  "Integration test that requires an S3 versioned bucket location.

  This allows integration testing of version restore. It sets up a directory in
  a versioned bucket, restores, verifies, and cleans up. It can be used in one
  of two ways:

  1. Simple--run a complete test with a local mapreduce in one command.

     lein run -m s3-dist-versions.integration-test full-test s3://versioned-bucket/temp-path

  2. Flexible--run each stage separately.

     lein run -m s3-dist-versions.integration-test setup s3://versioned-bucket/temp-path
     # Setup outputs a restore time. Then, run the s3-version-restore mapreduce
     # any way you like. We usually use lemur.
     lemur run lemur/example-jobdef.clj \\
               --src s3://versioned-bucket/temp-path \\
               --dest s3://versioned-bucket/temp-path \\
               --restore-time 2014-01-01T12:00:00Z \\
               --version-info-output s3://versioned-bucket/temp-path-versions \\
               --delete
     lein run -m s3-dist-versions.integration-test check s3://versioned-bucket/temp-path
     lein run -m s3-dist-versions.integration-test clean s3://versioned-bucket/temp-path

  This file is in src/ rather than test/ because it requires a versioned bucket
  configured from the commandline, so it needs to be run manually."
  (:require [amazonica.aws.s3 :as aws-s3]
            [clojure.data :as data]
            [clojure.java.io :as jio]
            [s3-dist-versions.main :as main]
            [s3-dist-versions.s3 :as s3]
            [s3-dist-versions.utils :as utils]))


(def s3-operations
  "An example set of S3 operations and a restore marker."
  [;; a is simple--never modified
   [:put "a" "a"]
   ;; b is revised before the restore target
   [:put "b" "b"]
   [:put "b" "b-revised"]
   ;; c is deleted before the restore target
   [:put "c" "c"]
   [:delete "c"]
   ;; d is deleted after the restore target
   [:put "d" "d"]
   ;; e is revised after the restore target
   [:put "e" "e"]
   ;; f is revised before and after the restore target
   [:put "f" "f"]
   [:put "f" "f-revised"]
   ;; g is deleted before the restore target and recreated after
   [:put "g" "g"]
   [:delete "g"]
   ;; h is deleted and recreated before the restore target
   [:put "h" "h"]
   [:delete "h"]
   [:put "h" "h-recreated"]
   ;; Here's the target time where we'll restore; we should see:
   ; a, b-revised, d, e, f-revised, h-recreated
   [:restore]
   [:delete "d"]
   [:put "e" "e-revised"]
   [:put "f" "f-double-revised"]
   [:put "g" "g-recreated"]
   ;; y is created after the restore target
   [:put "y" "y"]
   ;; z is created and deleted after the restore target
   [:put "z" "z"]
   [:delete "z"]])

(def s3-target-contents
  "What we expect to see after a restore."
  (->> s3-operations
       (take-while #(not= [:restore] %))
       (reduce
         (fn [m [op sub-path contents]]
           (condp = op
             :put (assoc m sub-path contents)
             :delete (dissoc m sub-path)
             m))
         {})))

(defn put-object!
  "Put a string into an S3 object."
  [bucket path contents]
  (with-open [is (-> contents
                     (.getBytes "UTF-8")
                     java.io.ByteArrayInputStream.)]
    (aws-s3/put-object bucket path is {:content-length (count contents)})))

(defn delete-object!
  "Delete an object and return its delete marker."
  [bucket path]
  (-> (aws-s3/delete-objects {:bucket-name bucket :keys [path]})
      :deleted-objects
      first))

(defn get-object
  "Get a string from an S3 object."
  [bucket path]
  (try
    (with-open [stream (:input-stream (aws-s3/get-object bucket path))]
      (slurp stream))
    ;; Nonexistent objects throw exceptions--treat them as nil.
    (catch com.amazonaws.services.s3.model.AmazonS3Exception e
      nil)))

(defn get-version-time
  "Get the last-modified time of a version."
  [bucket key- version-id]
  (->> (s3/versions bucket key-)
       (filter #(= version-id (:version-id %)))
       first
       :last-modified))

(defn check!
  "Do the contents of the S3 bucket match the expected?"
  [bucket path]
  (let [[extra missing both]
        (data/diff
          (apply merge
                 (for [{key- :key} (s3/objects bucket path)]
                   {key- (get-object bucket key-)}))
          (apply merge
                 (for [[k v] s3-target-contents]
                   {(str path k) v})))
        success? (every? empty? [extra missing])]
    (if success?
      (println "Check successful!")
      (println (format "Check failed:\n  extra: %s\n  missing: %s\n  both: %s"
                       extra missing both)))
    success?))

(defn s3-empty?
  "Is this S3 bucket/prefix empty of all preexisting versions?"
  [bucket prefix]
  (if (empty? (s3/versions bucket prefix))
    true
    (println (format "Error: there are old versions present at s3://%s/%s"
                     bucket prefix))))

(defn setup!
  "Perform s3-operations to prepare the test area in S3. Return the timestamp
  of the target restore time."
  [bucket path]
  (when (s3-empty? bucket path)
    (println (format "creating demo S3 versions at s3://%s/%s" bucket path))
    (let [versions (doall
                     (for [[op sub-path contents] s3-operations
                           :let [k (str path sub-path)]]
                       (do
                         ;; Sleep to give timestamps varying by at least 1
                         ;; second.
                         (Thread/sleep 1000)
                         [op k (condp = op
                                 :put (:version-id
                                        (put-object! bucket k contents))
                                 :delete (:delete-marker-version-id
                                           (delete-object! bucket k))
                                 nil)])))
          [_ target-path target-version] (->> versions
                                              ;; Get the last operation before
                                              ;; :restore
                                              (take-while
                                                #(not= (first %) :restore))
                                              last)
          restore-time (get-version-time bucket target-path target-version)]
      (println (format (str "created demo S3 versions at s3://%s/%s "
                            "with target restore time %s")
                       bucket path restore-time))
      restore-time)))

(defn clean-prompt!
  "Prompt the user for whether to really do 'clean'."
  [s3-uri]
  (print
    (format "Really delete all versions under \"%s\"?! [y/N] "
            s3-uri))
  (flush)
  (-> (read-line) .toUpperCase (#{"Y" "YES"})))

(defn clean!
  "CAUTION! Delete all S3 versions under this prefix."
  [bucket path prompt?]
  (let [s3-uri (format "s3://%s/%s" bucket path)]
    (if (or (not prompt?) (clean-prompt! s3-uri))
      (do
        (println "Deleting all versions under" s3-uri)
        (doseq [{:keys [key version-id]} (s3/versions bucket path)]
          (aws-s3/delete-version bucket key version-id)))
      (println "Aborting--not touching contents of" s3-uri))))

(defn run!
  "Run the mapreduce flow locally."
  [bucket path restore-time]
  (utils/with-temp-dir [temp-dir]
    (let [prefix-path (str temp-dir "/prefixes")
          output-path (str temp-dir "/output")]
      (spit prefix-path path)
      (main/run "--src" (str "s3://" bucket)
                "--prefixes" prefix-path
                "--restore-time" (str restore-time)
                "--version-info-output" output-path
                "--delete"))))

(defn full-test!
  "Set up S3, run the restoration flow, check the results, and clean up S3
  afterward."
  [bucket path]
  (when (s3-empty? bucket path)
    (try
      (when-let [restore-time (setup! bucket path)]
        (run! bucket path restore-time)
        (check! bucket path))
      (finally
        (clean! bucket path false)))))

(defn validate-args!
  "Check the arguments and parse the S3 URI."
  [task s3-uri]
  (when-not (and (#{"setup" "check" "full-test" "clean"} task)
                 s3-uri
                 (s3/s3-uri? s3-uri))
    (println "Usage: lein run -m s3-dist-versions.integration-test"
             "[setup|check|full-test|clean] s3-uri")
    (System/exit 2)))

(defn exit!
  "Translate a boolean success? into a UNIX exit code and exit with that code.
  I.e. (exit! true) exits with return code 0."
  [success?]
  (System/exit (if success? 0 1)))

(defn -main
  [& [task s3-uri]]
  (validate-args! task s3-uri)
  (let [[bucket path] (s3/parse-s3-uri s3-uri)]
    (exit!
      (condp = task
        "setup" (setup! bucket path)
        "check" (check! bucket path)
        "clean" (clean! bucket path true)
        "full-test" (full-test! bucket path)))))
