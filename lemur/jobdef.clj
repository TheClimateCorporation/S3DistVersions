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


;; Run this with lemur (https://github.com/TheClimateCorporation/lemur/)
;;
;; lemur run lemur/jobdef.clj

(defcluster s3-dist-versions-cluster
  :local {:base-uri "/tmp/hadoop-s3-dist-versions/${run-path}"}
  :ami-version "2.4.9"
  :visible-to-all-users true
  :jar-src-path "http://download.climate.com/s3distversions/releases/S3DistVersions-0.1.0.jar"
  ;; You need to set a keypair to SSH to the Hadoop master.
  ;; http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/EMR_SetUp_KeyPair.html
  :keypair "mykeypair"
  ;; You might need to set roles to access S3
  ;; http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-iam-roles.html
  ; :service-role "EMR_DefaultRole"
  ; :job-flow-role "my-job-role"
  ;; You can set some metadata
  :tags "emr:name=S3DistVersions"
  :app "S3DistVersions"
  ;; You need a place to put this logs and whatnot
  :bucket "my.bucket/my/emr/dir"
  ;; I hear that using a high-memory jobtracker reduces failures with many
  ;; mappers.
  :master-instance-type "m1.large"
  :slave-instance-type "m1.large"
  :num-instances 6
  ;; It may be worth turning up the number of mappers and reducers.
  ;; http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/TaskConfiguration.html
  :bootstrap-action.1 ["configure hadoop"
                       "s3://elasticmapreduce/bootstrap-actions/configure-hadoop"
                       ["-m" "mapred.tasktracker.map.tasks.maximum=6"
                        "-m" "mapred.tasktracker.reduce.tasks.maximum=6"]]
  ;; For some reason, I've observed failures using debugging, so I have
  ;; disabled it.
  :enable-debugging? false)

(defstep s3-dist-versions-step
  :args.src "s3://my-bucket/source/path"
  :args.prefixes nil
  :args.dest nil
  :args.restore-time "YYYY-MM-DDThh-mm-ssZ"  ; ISO-8601 datetime
  :args.version-info-output "${data-uri}/version-info"
  :args.delete false)

(fire!
  s3-dist-versions-cluster
  s3-dist-versions-step)
