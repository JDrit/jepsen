(defproject jepsen.raft "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-localrepo "0.5.3"]]
  :aot :all
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [jepsen "0.0.6"]
                 [jdb-jepsen "0.1.0-SNAPSHOT"]])
