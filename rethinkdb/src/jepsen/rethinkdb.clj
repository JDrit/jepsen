(ns jepsen.rethinkdb
  (:refer-clojure :exclude [run!])
  (:require [clojure [pprint :refer :all]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh timeout retry]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [model     :as model]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [store     :as store]
                    [report    :as report]
                    [tests     :as tests]]
            [jepsen.control [util :as cu]]
            [jepsen.os.debian :as debian]
            [jepsen.checker.timeline :as timeline]
            [rethinkdb.core :refer [connect close]]
            [rethinkdb.query :as r]
            [knossos.core :as knossos]
            [cheshire.core :as json])
  (:import (clojure.lang ExceptionInfo)))

(def log-file "/var/log/rethinkdb")

(defn faketime-script
  "A sh script which invokes cmd with a faketime wrapper."
  [cmd]
  (str "#!/bin/bash\n"
       "faketime -m -f \"+$((RANDOM%100))s x1.${RANDOM}\" "
       cmd
       " \"$@\""))

(defn faketime-wrapper!
  "Replaces an executable with a faketime wrapper. Idempotent."
  [cmd]
  (let [cmd'    (str cmd ".no-faketime")
        wrapper (faketime-script cmd')]
    (when-not (cu/file? cmd')
      (info "Installing faketime wrapper.")
      (c/exec :mv cmd cmd')
      (c/exec :echo wrapper :> cmd)
      (c/exec :chmod "a+x" cmd))))

(defn install!
  "Install RethinkDB on a node"
  [node version]
  ; Install package
  (debian/add-repo! "rethinkdb"
                    "deb http://download.rethinkdb.com/apt jessie main")
  (c/su (c/exec :wget :-qO :- "https://download.rethinkdb.com/apt/pubkey.gpg" |
                :apt-key :add :-))
  (debian/install {"rethinkdb" version})
  (faketime-wrapper! "/usr/bin/rethinkdb")

  ; Set up logfile
  (c/exec :touch log-file)
  (c/exec :chown "rethinkdb:rethinkdb" log-file))

(defn join-lines
  "A string of config file lines for nodes to join the cluster"
  [test]
  (->> test
       :nodes
       (map (fn [node] (str "join=" (name node) :29015)))
       (clojure.string/join "\n")))

(defn configure!
  "Set up configuration files"
  [test node]
  (info "Configuring" node)
  (c/su
    (c/exec :echo (-> "jepsen.conf"
                      io/resource
                      slurp
                      (str "\n" (join-lines test))
                      (str "\n\n" "server-name=" (name node)))
            :> "/etc/rethinkdb/instances.d/jepsen.conf")))

(defn start!
  "Starts the rethinkdb service"
  [node]
  (c/su
    (info node "Starting rethinkdb")
    (c/exec :service :rethinkdb :start)
    (info node "Started rethinkdb")))

(defn conn
  "Open a connection to the given node."
  [node]
  (connect :host (name node) :port 28015))

(defn wait-for-conn
  "Wait until a connection can be opened to the given node."
  [node]
  (info "Waiting for connection to" node)
  (retry 5 (close (conn node)))
  (info node "ready"))

(defn run!
  "Like rethinkdb.query/run, but asserts that there were no errors."
  [query conn]
  (let [result (r/run query conn)]
    (when (contains? result :errors)
      (assert (zero? (:errors result)) (:first_error result)))
    result))

(defn db
  "Set up and tear down RethinkDB"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (install! node version)
      (configure! test node)
      (start! node)

      (wait-for-conn node))

    (teardown! [_ test node]
      (info node "Nuking" node "RethinkDB")
      (cu/grepkill! "rethinkdb")
      (c/su
        (c/exec :rm :-rf "/var/lib/rethinkdb/jepsen")
        (c/exec :truncate :-c :--size 0 log-file))
      (info node "RethinkDB dead"))

    db/LogFiles
    (log-files [_ test node] [log-file])))

(defmacro with-errors
  "Takes an invocation operation, a set of idempotent operation
  functions which can be safely assumed to fail without altering the
  model state, and a body to evaluate. Catches RethinkDB errors and
  maps them to failure ops matching the invocation."
  [op idempotent-ops & body]
  `(let [error-type# (if (~idempotent-ops (:f ~op))
                       :fail
                       :info)]
     (try
       ~@body
       (catch clojure.lang.ExceptionInfo e#
         (case (:e (ex-data e#))
           4100000 (assoc ~op :type :fail,       :error (:cause (ex-data e#)))
                   (assoc ~op :type error-type#, :error (str e#)))))))

(defn primaries
  "All nodes that think they're primaries for the given db and table"
  [nodes db table]
  (->> nodes
       (pmap (fn [node]
               (-> (r/db db)
                   (r/table table)
                   (r/status)
                   (run! (conn node))
                   :shards
                   (->> (mapcat :primary_replicas)
                        (some #{(name node)}))
                   (when node))))
       (remove nil?)))

(defn std-gen
  "Takes a client generator and wraps it in a typical schedule and nemesis
  causing failover."
  [gen]
  (gen/phases
    (->> gen
         (gen/nemesis
           (gen/seq (cycle [(gen/sleep 5)
                            {:type :info :f :start}
                            (gen/sleep 5)
                            {:type :info :f :stop}])))
         (gen/time-limit 500))))

(defn test-
  "Constructs a test with the given name prefixed by 'rethinkdb ', merging any
  given options."
  [name opts]
  (merge
    (assoc tests/noop-test
           :name      (str "rethinkdb " name)
           :os        debian/os
           :db        (db (:version opts))
           :model     (model/cas-register)
           :checker   (checker/compose {:linear checker/linearizable
                                        :perf   (checker/perf)})
;           :nemesis   (nemesis/hammer-time #(primaries % "jepsen" "cas")
;                                           "rethinkdb"))
           :nemesis   (nemesis/partition-random-halves))
    (dissoc opts :version)))
