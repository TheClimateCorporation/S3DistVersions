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

(ns s3-dist-versions.test.s3
  (:require [clojure.test :refer :all]
            [amazonica.aws.s3 :as aws-s3]
            [clojure.set :as set]
            [s3-dist-versions.s3 :as v-s3]))


(deftest test-s3-uri?
  (is (true? (v-s3/s3-uri? "s3://bucket/wat")))
  (is (true? (v-s3/s3-uri? "s3://bucket")))
  (is (true? (v-s3/s3-uri? "s3n://bucket")))
  (is (false? (v-s3/s3-uri? "http://bucket"))))

(deftest test-parse-s3-uri
  (is (= ["bucket" ""] (v-s3/parse-s3-uri "s3://bucket")))
  (is (= ["bucket" ""] (v-s3/parse-s3-uri "s3://bucket/")))
  (is (= ["bucket" "abc"] (v-s3/parse-s3-uri "s3://bucket/abc")))
  (is (= ["bucket" "abc/de/"] (v-s3/parse-s3-uri "s3://bucket/abc/de/"))))

(deftest test-lazy-mapcat
  (testing "lazy-mapcat is lazy"
    (let [;; Track what data we've realized
          realized (atom [])
          ;; Make an infinite sequence and track what has been realized
          data (map #(do (swap! realized conj %) %) (map range (iterate inc 1)))
          ;; Try to realize just the first element
          taken (doall (take 1 (#'v-s3/lazy-mapcat identity data)))]
      ;; Check that we've realized just the first element
      (is (= @realized '[(0)])))))

(def fake-data
  "A list of fake response maps."
  (for [i (range 11)]
    {:data (map (partial + (* i 10)) (range 10))
     :i i
     :truncated? (< i 10)}))

(def first-fake-data
  "A fake first request function that returns from fake-data."
  (constantly (first fake-data)))

(defn more-fake-data
  "A fake next-batch request function that returns from fake-data."
  [{:keys [i truncated?] :as req}]
  (is truncated?)
  (nth fake-data (inc i)))

(defn rename-data
  "Rename fake data's :data key to another key."
  [key- data-fn]
  (comp #(set/rename-keys % {:data key-}) data-fn))

(deftest test-lazy-aws-list
  (is (= (range 110)
         (#'v-s3/lazy-aws-list
           first-fake-data more-fake-data :data ::request))))

(deftest test-objects
  (with-redefs [aws-s3/list-objects
                (fn [req]
                  (is (nil? (:delimiter req)))
                  ((rename-data :object-summaries first-fake-data) req))
                aws-s3/list-next-batch-of-objects
                (rename-data :object-summaries more-fake-data)]
    (is (= (range 110)
           (v-s3/objects ::bucket ::request)))))

(deftest test-dirs
  (with-redefs [aws-s3/list-objects
                (fn [req]
                  (is (= "/" (:delimiter req)))
                  ((rename-data :common-prefixes first-fake-data) req))
                aws-s3/list-next-batch-of-objects
                (rename-data :common-prefixes more-fake-data)]
    (is (= (range 110)
           (v-s3/dirs ::bucket ::request)))))

(deftest test-versions
  (with-redefs [aws-s3/list-versions
                (fn [req]
                  (is (nil? (:delimiter req)))
                  ((rename-data :version-summaries first-fake-data) req))
                aws-s3/list-next-batch-of-versions
                (rename-data :version-summaries more-fake-data)]
    (is (= (range 110)
           (v-s3/versions ::bucket ::request)))))

(deftest test-switch-prefixes
  (is (= "y/a" (v-s3/switch-prefixes "x" "y" "x/a")))
  (is (= "y/a" (v-s3/switch-prefixes "" "y/" "a")))
  (is (= "y/a" (v-s3/switch-prefixes nil "y/" "a")))
  (is (= "a" (v-s3/switch-prefixes "x/" "" "x/a")))
  (is (= "a" (v-s3/switch-prefixes "x/" nil "x/a")))
  (is (= "x/a" (v-s3/switch-prefixes nil nil "x/a")))
  (is (thrown? IllegalArgumentException
               (v-s3/switch-prefixes "x" "y" "a/a"))))

(deftest test-restore-version
  (let [request-keys [:source-bucket-name
                      :source-key
                      :source-version-id
                      :destination-bucket-name
                      :destination-key]
        copy-request {:source-bucket-name :source-bucket-name
                      :source-key "prefix/key"
                      :source-version-id :source-version-id
                      :destination-bucket-name :destination-bucket-name
                      :destination-key "destination-prefix/key"}
        req-atom (atom [])]
    (with-redefs [aws-s3/delete-object (fn [& req] (swap! req-atom conj
                                                          (cons :delete req)))
                  aws-s3/copy-object (fn [req] (swap! req-atom conj req))]
      (testing "Version restore copy"
        (v-s3/restore-version {:key "prefix/key"
                               :version-id :source-version-id
                               :bucket-name :source-bucket-name}
                              :source-bucket-name
                              "prefix/"
                              :destination-bucket-name
                              "destination-prefix/"
                              false)
        (is (= [copy-request] @req-atom))
        (reset! req-atom []))
      (testing "Version restore delete with deletion enabled"
        (v-s3/restore-version {:key "prefix/key"
                               :version-id nil
                               :delete-marker? true
                               :bucket-name :source-bucket-name}
                              :source-bucket-name
                              "prefix/"
                              :destination-bucket-name
                              "destination-prefix/"
                              true)
        (is (= ['(:delete
                   :destination-bucket-name
                   "destination-prefix/key")]
               @req-atom))
        (reset! req-atom []))
      (testing "Version restore delete with deletion disabled"
        (v-s3/restore-version {:key "prefix/key"
                               :version-id nil
                               :delete-marker? true
                               :bucket-name :source-bucket-name}
                              :source-bucket-name
                              "prefix/"
                              :destination-bucket-name
                              "destionation-prefix/"
                              false)
        (is (= [] @req-atom))
        (reset! req-atom [])))))
