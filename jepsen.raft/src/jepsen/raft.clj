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
(def log-file "/tmp/jepsen.log")

(defn client-addr-url [node]
  (str (name node) ":6000"))

(defn raft-addr-url [node]
  (str (name node) ":5000"))

(defn peers [test]
  (->> test
       :nodes
       (map raft-addr-url)
       (str/join ",")))

(defn servers [test]
  (->> test
       :nodes
       (map client-addr-url)
       (str/join ",")))

(defn start-jdb!
  [test node]
  (info node "starting jdb")
  (c/exec :java
          :-jar (cu/wget! "http://csh.rit.edu/~jd/jdb.jar")
          :-client (client-addr-url node)
          :-raftAddr (raft-addr-url node)
          :-peers (peers test)
          :-servers (servers test)
          :-pid pid-file
          :-log.level "DEBUG"
          :-log.output "/tmp/output.log"
          (c/lit " &")))

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn db []
  (reify db/DB
    (setup! [_ test node]
      (c/cd "/tmp"
            (let [jar (cu/wget! "http://csh.rit.edu/~jd/jdb.jar")] 
              (info node "starting jdb...")
              (c/exec :start-stop-daemon :--start
                      :--background
                      :--make-pidfile
                      :--pidfile pid-file
                      :--chdir "/tmp"
                      :--exec "/usr/bin/java"
                      :--no-close
                      :--
                      :-jar jar
                      :-client (client-addr-url node)
                      :-raftAddr (raft-addr-url node)
                      :-peers (peers test)
                      :-servers (servers test)
                      :-log.level "INFO"
                      :-log.output "/tmp/output.log"
                      :>> log-file
                      (c/lit "2>&1"))
              (Thread/sleep (* 10 1000))
              (info node "jdb started"))))

    (teardown! [_ test node]
      (cu/grepkill "jdb.jar")
      (c/exec :rm "/tmp/jdb.jar")
      (c/exec :rm "/tmp/output.log")
      (c/exec :rm log-file)
      (info node "jdb nuked"))))

(defrecord CASClient [k client]
  client/Client
  (setup! [this test node]
    (info node (str "CAS client generated " (name node)))
    (let [client (jdb/connect (str "http://" (client-addr-url node)) (uuid))]
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

        (catch [:status 404] e
          (assoc op :type fail :value :404))

        (catch [:status 400] e
          (assoc op :type fail :value :400))

        (catch (and (instance? clojure.lang.ExceptionInfo %)) e
          (assoc op :type fail :value e))

        (catch (and (:errorCode %) (:message %)) e
          (assoc op :type fail :value e)))))

    (teardown! [_ test]))

(defn cas-client
  "A compare and set register"
  []
   (CASClient. "jepsen" nil))


