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

(ns s3-dist-versions.test.main
  (:require [clojure.test :refer :all]
            [clj-time.core :as ct]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [s3-dist-versions.main :as main]
            [s3-dist-versions.s3 :as v-s3]
            [s3-dist-versions.test.helpers :refer [mktime]]
            [s3-dist-versions.utils :as utils]))


(def versions
  {"a/b/c" (range 1 10)
   "a/b/d" (range 1 20)
   "a/be" [0]
   "xx/vv" (range 10 20)
   "vvvvv" [100 200]
   "zzzzz" [10 20 100]})

(def restore-time (mktime 15))

(def restore-versions
  {"a/b/c" 9
   "a/b/d" 15
   "a/be" 0
   "xx/vv" 15
   "vvvvv" :delete
   "zzzzz" 10})

(def prefixes
  ["a/b"
   "xx"
   "v"
   "zz"])

(defn list-versions
  "Mock list-versions function"
  [_ prefix]
  (let [re (re-pattern (str "^" prefix ".*"))]
    (for [[k vs] versions
          :when (re-matches re k)
          v vs]
      {:key k
       :bucket-name "BUCKET"
       :version-id v
       :last-modified (mktime v)})))

(defn expected-restorations
  [delete?]
  (for [[k v] restore-versions
        :when (not= v (last (get versions k)))]
    [(if (= v :delete)
       {:key k
        :bucket-name "BUCKET"
        :version-id nil
        :delete-marker? true
        :last-modified nil
        :is-current false}
       {:key k
        :bucket-name "BUCKET"
        :version-id v
        :last-modified (mktime v)
        :is-current false})
     "BUCKET"
     ""
     "BUCKET"
     ""
     delete?]))

(deftest test-configured-main
  (doseq [delete? [false true]]
    (let [restored (atom [])]
      (utils/with-temp-dir [tempdir]
        (with-redefs [v-s3/versions list-versions
                      v-s3/restore-version (fn [& args]
                                             (swap! restored conj args)
                                             "restored")]
          (let [output-dir (str tempdir "/output")
                prefix-file (str tempdir "/prefixes.txt")
                _ (spit prefix-file
                        (str/join "" (map #(str % "\n") prefixes)))
                results (utils/tool-run
                          main/configured-main
                          (concat
                            ["--src" "s3://BUCKET"
                             "--prefixes" prefix-file
                             "--restore-time" (str (mktime 15))
                             "--version-info-output" output-dir]
                            (when delete? ["--delete"])))]
            (is (= (set (expected-restorations delete?))
                   (set @restored)))))))))

(deftest test-configured-main-help
  (is (re-find #"usage"
               (with-out-str
                 (is (= 2 (utils/tool-run main/configured-main ["--help"])))))))
