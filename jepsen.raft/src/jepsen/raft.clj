(ns jepsen.raft
  "Tests for jdb"
   (:require [clojure.tools.logging :refer :all]
             [clojure.core.reducers :as r]
             [clojure.java.io :as io]
             [clojure.string :as str]
             [clojure.pprint :refer [pprint]]
             [knossos.op :as op]
             [jepsen [client :as client]
                     [core :as jepsen]
                     [db :as db]
                     [tests :as tests]
                     [control :as c :refer [|]]
                     [checker :as checker]
                     [nemesis :as nemesis]
                     [generator :as gen]
                     [util :refer [timeout meh]]]
             [jepsen.control.util :as cu]
             [jepsen.control.net :as cn]
             [jepsen.os.debian :as debian]))

(defn db []
  (let [running (atom nil)]
    (reify db/DB
      (setup! [this test node]
        ))))

(defn basic-test
  "A simple test of raft"
  [version]
  tests/noop-test)
