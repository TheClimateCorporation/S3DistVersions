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

(ns s3-dist-versions.utils
  "Minor utility functions.")


(defmacro with-temp-dir
  "With safe cleanup, use a temp dir.

  e.g. (with-temp-dir [d] your-code-here)"
  [[d] & body]
  (assert (symbol? d))
  `(let [file# (java.nio.file.Files/createTempDirectory
                 "s3-dist-versions"
                 ;; [sigh] Oh, Java.
                 (make-array java.nio.file.attribute.FileAttribute 0))
         ~d (str file#)]
     (try ~@body
          (finally (org.apache.commons.io.FileUtils/deleteDirectory
                     (java.io.File. (str file#)))))))

(defn seq-to-array
  "Convert a seq to a Java array of the given type."
  [type- seq-]
  (let [arr (make-array type- (count seq-))]
    (doseq [[i x] (map-indexed vector seq-)]
      (aset arr i x))
    arr))

(defn- make-tool
  "Make a Hadoop Tool that calls a function."
  [f]
  (proxy [org.apache.hadoop.conf.Configured
          org.apache.hadoop.util.Tool] []
    (run [args-array]
      (f this (seq args-array)))))

(defn tool-run
  "Run a function parsing all the Hadoop standard arguments out as per
  https://hadoop.apache.org/docs/r2.5.1/api/org/apache/hadoop/util/Tool.html"
  [f args]
  (org.apache.hadoop.util.ToolRunner/run
    (org.apache.hadoop.conf.Configuration.)
    (make-tool f)
    (seq-to-array String args)))
