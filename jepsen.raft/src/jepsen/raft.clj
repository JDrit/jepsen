(ns jepsen.raft
  "Tests for jdb"
  (:require [clojure.tools.logging    :refer [debug info warn]]
            [clojure.java.io          :as io]
            [clojure.string           :as str]
            [jepsen.core              :as core]
            [jepsen.util              :refer [meh timeout]]
            [jepsen.codec             :as codec]
            [jepsen.core              :as core]
            [jepsen.control           :as c]
            [jepsen.control.net       :as net]
            [jepsen.control.util      :as cu]
            [jepsen.client            :as client]
            [jepsen.db                :as db]
            [jepsen.generator         :as gen]
            [jepsen.os.debian         :as debian]
            [knossos.core             :as knossos]
            [cheshire.core            :as json]
            [slingshot.slingshot      :refer [try+]]
            [jdb-jepsen.core          :as jdb]))
  
(def pid-file "/tmp/jdb.pid")

(defn client-addr-url [node]
  (str "http://" (name node) ":6000"))

(defn raft-addr-url [node]
  (str "http://" (name node) ":5000"))

(defn peers
  "The command line list of raft peers"
  [test]
  (->> test
       :nodes
       (map raft-addr-url)
       (str/join ",")))

(defn start-jdb!
  [test node]
  (info node "starting jdb")
  (cu/wget! "http://csh.rit.edu/~jd/jdb.jar")
  (c/exec :java
          :-jar :jdb.jar
          :-client (client-addr-url node)
          :-raftAddr (raft-addr-url node)
          :-peers (peers node)
          :-pid pid-file
          (c/lit "2>&1")))

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn db []
  (let [running (atom nil)]
    (reify db/DB
      (setup! [this test node]
        (start-jdb! test node)
        (info node "jdb started"))

      (teardown! [_ test node]
        (meh (c/exec :kill :-9 (slurp pid-file)))
        (c/exec :rm pid-file)
        (info node "jdb nuked")))))

(defrecord CASClient [k client]
  client/Client
  (setup! [this test node]
    (let [client (jdb/connect (str "http://" (name node)) (uuid))]
      (jdb/put! client k (json/generate-string nil))
      (assoc this :client client)))

  (invoke! [this test op]
    ; Reads are idempotent; if they fail we can always assume they didn't
    ; happen in the history, and reduce the number of hung processes, which
    ; makes the knossos search more efficient
    (let [fail (if (= :read (:f op))
                :fail
                :info)]
      (try+
        (case (:f op)
          :read (let [value (-> client
                                (jdb/get k)
                                (json/parse-string true))]
                  (assoc op :type :ok :value value))
          :write (do (->> (:value op)
                          json/generate-string
                          (jdb/put! client k))
                   (assoc op :type :ok))
          :cas (let [[value value'] (:value op)
                     ok?            (jdb/cas! client k 
                                              (json/generate-string value)
                                              (json/generate-string value'))]
                 (assoc op :type (if ok? :ok :fail))))
        (catch java.net.SocketTimeoutException e
          (assoc op :type fail :value :timed-out))

        (catch [:status 307] e
          (assoc op :type fail :value :redirect-loop))

        (catch (and (instance? clojure.lang.ExceptionInfo %)) e
          (assoc op :type fail :value e))

        (catch (and (:errorCode %) (:message %)) e
          (assoc op :type fail :value e)))))

    (teardown! [_ test]))

(defn cas-client
  "A compare and set register"
  []
   (CASClient. "jepsen" nil))
