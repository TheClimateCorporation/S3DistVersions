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

(ns s3-dist-versions.s3
  "S3 helper functions, mostly for turning paged queries into nice lazy lists."
  (:require [amazonica.aws.s3 :as s3]
            [clojure.tools.logging :as log]))


(defn s3-uri?
  "Check if the input is an S3 URI."
  [s3-uri]
  (boolean (re-matches #"s3n?://.*" s3-uri)))

(defn parse-s3-uri
  "Parse a bucket and path from an s3://bucket/path URI."
  [s3-uri]
  (let [[_ bucket _ path] (re-matches #"s3n?://([^/]*)(/(.*))?" s3-uri)]
    [bucket (or path "")]))

(defn- lazy-mapcat
  "Mapcat, but lazier. Built-in mapcat seems to evaluate more of the sequence
  than necessary."
  [f s]
  (lazy-seq
    (when (seq s)
      (concat (f (first s))
              (lazy-mapcat f (rest s))))))

(defn- lazy-aws-list
  "Given an access function that is paged, a function to get the next page, and
  an access function to get the important results from the AWS response, lazily
  get all the results for the request."
  [get-first-fn get-more-fn access-fn request]
  (->> request
       get-first-fn
       (iterate #(when (:truncated? %) (get-more-fn %)))
       (take-while (complement nil?))
       (lazy-mapcat access-fn)))

(defn objects
  "Lazily get all objects from S3 for some bucket and prefix."
  [bucket prefix]
  (lazy-aws-list
    s3/list-objects s3/list-next-batch-of-objects :object-summaries
    {:bucket-name bucket
     :prefix prefix
     :delimiter nil
     :max-keys 1000}))

(defn dirs
  "Lazily get all dirs from S3 for some bucket and prefix."
  [bucket prefix]
  (lazy-aws-list
    s3/list-objects s3/list-next-batch-of-objects :common-prefixes
    {:bucket-name bucket
     :prefix prefix
     :delimiter "/"
     :max-keys 1000}))

(defn versions
  "Lazily get all versions from S3 for some bucket and prefix."
  [bucket prefix]
  (lazy-aws-list
    s3/list-versions s3/list-next-batch-of-versions :version-summaries
    {:bucket-name bucket
     :prefix prefix
     :delimiter nil
     :max-keys 1000}))

(defn switch-prefixes
  "Switch src-prefix to dest-prefix in key-."
  [src-prefix dest-prefix key-]
  (let [src-prefix (or src-prefix "")
        dest-prefix (or dest-prefix "")]
    (when (not= src-prefix (subs key- 0 (count src-prefix)))
      (throw (IllegalArgumentException.
               (format "src-prefix %s is not a prefix of key %s"
                       src-prefix key-))))
    (str dest-prefix (subs key- (count src-prefix)))))

(defn restore-version
  "Given a version record, restore that version from
  {src-bucket}/{src-prefix}{key} to {dest-bucket}/{dest-prefix}{key}. If the
  version record is a delete marker and delete? is true, then delete
  {dest-bucket}/{dest-prefix}{key}.

  Version records look like:
   {:key \"path/to/key\",
    :storage-class \"STANDARD\",
    :version-id \"null\",
    :etag \"d69bea6b3e3e9423fef3db14cc0467e8de430c7e\",
    :last-modified #<DateTime 2014-09-26T07:13:35.000-07:00>,
    :size 52309,
    :latest? true,
    :bucket-name \"com.climate.bucket\",
    :delete-marker? false,
    :owner {:id \"c6e69ff7f27252b902e3401633fced5eec5b114c\",
            :display-name \"climate-corp\"}}"
  [version src-bucket src-prefix dest-bucket dest-prefix delete?]
  (let [{version-id :version-id key- :key} version
        dest-key (switch-prefixes src-prefix dest-prefix key-)]
    (if (:delete-marker? version)
      (when delete?
        (log/infof "deleting s3://%s/%s" dest-bucket dest-key)
        (s3/delete-object dest-bucket dest-key))
      (do
        (log/infof "restoring s3://%s/%s at version %s to s3://%s/%s"
                   src-bucket key- version-id dest-bucket dest-key)
        (s3/copy-object
          {:source-bucket-name src-bucket
           :source-key key-
           :source-version-id version-id
           :destination-bucket-name dest-bucket
           :destination-key dest-key})))))
