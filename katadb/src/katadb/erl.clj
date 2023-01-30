(ns katadb.erl
   {:doc "Erlang like set of primitives. I am reading a book, so here is that

          I DONT PRENTEND THAT I KNOW WHAT I AM DOING!

          If you know the name of a process you can send it a message
          Message passing is the only way for processes to interact
          Error handling is non-local
          Processes do what they are supposed to do or fail.
 "}
  (:require [clojure.core.async :as async])
  (:gen-class))

(defonce ^:dynamic -STORAGE (atom {}))

(def -STATUS-FN
  {::run    #(assoc % ::error nil)
   ::error  identity
   ::new    identity})


(defn -make-pid
  []
  (rand-int Integer/MAX_VALUE))


(defn -make-channel
  [{:keys [capacity]}]
  (async/chan (or capacity 1)))


(defn -must-exist
  [pid]
  (assert (contains? @-STORAGE pid)))


(defn -must-not-exist
  [pid]
  (assert (not (contains? @-STORAGE pid))))


(defn -get-status
  [pid]
  (-must-exist pid)
  (get-in @-STORAGE [pid ::status]))


(defn -must-run
  [pid]
  (assert (= (-get-status pid) ::run)))


(defn -running?
  [pid]
  (= (-get-status pid) ::run))


(defn -must-not-run
  [pid]
  (assert (not= (-get-status pid) ::run)))


(defn -set-status!
  [pid status]
  (-must-exist pid)
  (update @-STORAGE pid
    (fn [val]
      (let [status-fn (get -STATUS-FN status)]
        (assert (not (nil? status-fn)))
        (status-fn (assoc val ::status status))))))


(defn -set-error!
  [pid error]
  (-must-exist pid)
  (-set-status! pid ::error)
  (update-in @-STORAGE [pid ::error] error))


(defn -run!
  [pid]
  (-must-exist pid)
  (let [{:keys [proc-fn
                chan]} (get @-STORAGE pid)]
    (-must-not-run pid)
    (-set-status! pid ::run)
    (async/go (loop []
                (let [msg (async/<! chan)]
                  
                  (try
                    (proc-fn msg)
                    (catch Exception e
                      (-set-error! pid e)))
                  
                  (when (-running? pid) (recur)))))))


(defn -make
  [proc-fn opts]
  {::pid     (-make-pid)
   ::chan    (-make-channel opts)
   ::proc-fn proc-fn
   ::opts    opts
   ::status  ::new})

;; ===================== public API ====================

(defn make-process
  "proc-fn should take reasonable time to complete
   should not eat all the memory
   should not hog all the CPU
   and, in general, should behave well,
   this is not Erlang"
  ([proc-fn]
   (make-process proc-fn nil))
  ([proc-fn opts]
   (let [{:keys [pid]
          :as   process} (-make proc-fn opts)]
     (-must-not-exist pid)
     (assoc @-STORAGE pid process)
     (-run! pid)
     pid)))


(defn kill!
  "TODO: better have nice error message"
  [pid]
  (-set-error! pid nil))


(defn restart!
  [pid]
  (kill! pid)
  (-run! pid))


(defn get-error
  [pid]
  (->> @-STORAGE (get pid) ::error))


(defn alive?
  [pid]
  (= (-get-status pid) ::run))


(defn send!
  [pid msg]
  (-must-exist pid)
  (let [chan (->> @-STORAGE (get pid) ::chan)]
    (async/>!! chan msg)))


(defn api-basic-test
  []
  (let [echo-stat (atom nil)
        pid      (make-process (fn [msg]
                                 (reset! echo-stat msg)))]
    (assert (= @echo-stat nil))
    
    (assert (= (alive? pid) true))
    
    (doseq [r (range 1000)]
      (send! pid r)
      (assert (= @echo-stat r)))

    (kill! pid)
    (assert (= (alive? pid) false))
    
    (send! pid "dead message")
    (assert (not= @echo-stat "dead message"))


    (restart! pid)
    (assert (= (alive? pid) true))

    (assert (= @echo-stat "dead message"))))
