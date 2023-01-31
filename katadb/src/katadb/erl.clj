(ns katadb.erl
   {:doc "Erlang like set of primitives. I am reading a book, so here is that

          I DONT PRENTEND THAT I KNOW WHAT I AM DOING!

          If you know the name of a process you can send it a message
          Message passing is the only way for processes to interact
          Error handling is non-local
          Processes do what they are supposed to do or fail.

Citing Page 27, six rules for fault tolerant system:

* Concurrency — Our system must support concurrency. The computational ecort needed to create or destroy a concurrent process should be very small, and there should be no penalty for creating large numbers of concurrent processes.
* Error encapsulation — Errors occurring in one process must not be able to damage other processes in the system.
* Fault detection — It must be possible to detect exceptions both locally (in the processes where the exception occurred) and remotely (we should be able to detect that an exception has occurred in a non-local process).
* Fault identification — We should be able to identify why an exception occurred.
* Code upgrade — there should exist mechanisms to change code as it is executing, and without stopping the system.
* Stable storage — we need to store data in a manner which survives a system crash


Sidelines: 

We are assuming unreliable message passing. 
What to do if QUEUE is full? Should we block? Should we discard? 

In theory, full queue is a fault, and we assume unreliable message passing. 
So the process should die, queue should be discarded. Supervisor/other process
can restart using larger capacity. 


I am thinking about JSON/HTTP interface for sending message to remote machine. Like, pid must include remote(ip?) address
So, it is possible to execute local(this machine ip + pid) and remote code. I think the easiest way is to provide http
interface for process manipulation, you can call this machine interface and remote machine interface. 

Setting up a node is basically winding up a JVM with http handler. 
Using JSON/HTTP allows seamless intergation with whatever you like.

Sample API 

Running old and new code at the same time can be tricky, I want to make it explicit, in general, 
we can use git tag to know 100% what kind of code is runnable on node 
Node might be deployed long ago, might be depolyed right now, code should explicitly call new code
I might be talking out of my a**, but something along these lines, we need explicit version dependency 
of current runnable in order to be able to <Code upgrade>
When node is deployed, it should have its tag (deployment id) explicit. 
Thus, we know, when we call old code and new code. 
To migrate: have at least two nodes, send code to new version

GET  /runnables/         list of functions that node can run 
GET  /version            returns deployment id, current git tag
POST /version/spawn      (returns pid)
POST /version/pid/restart will restart if dead, do nothing if running
POST /version/pid/send   (msg|shutdown) sending shutdown code will stop process, but not immediately, because of Queue
POST /version/pid/watch  (callback-url) will call callback-url if process dies with {error: true, reason: \"dead\"} 
POST /version/pid/kill   will immediately kill process, disposing of process Queue

 "}
   (:require [clojure.core.async :as async]
             [clojure.core.async.impl.protocols :as async.impl])
   (:import [java.util LinkedList]
            [clojure.lang Counted])
   (:gen-class))

;; TODO: kill should fully discard process/metadata/go blocks,
;;       although, it is possible to query status of pid

(def -DEFAULT-CAPACITY 100)

(defonce ^:dynamic -STORAGE (atom {}))
(defonce ^:dynamic -GHOSTS (atom {}))


(def -STATUS-FN
  {::run    #(assoc % ::error nil)
   ::error  identity})


(defn -make-pid
  []
  (rand-int Integer/MAX_VALUE))

(defn -drain!
  [^LinkedList buf]
  (doseq [b buf]
    (async.impl/remove!)))


(deftype DroppingFillableBuffer [^LinkedList buf ^long n]
  async.impl/UnblockingBuffer
  async.impl/Buffer
  (full? [_this]
    (>= (.size buf) n))
  (remove! [_this]
    (.removeLast buf))
  (add!* [this itm]
    (when-not (>= (.size buf) n)
      (.addFirst buf itm))
    this)
  (close-buf! [_this])
  Counted
  (count [_this]
    (.size buf)))


(defn -make-buffer
  [{:keys [capacity]}]
  (DroppingFillableBuffer. (LinkedList.) (or capacity DEFAULT-CAPACITY)))

(def -full? async.impl/full?)

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
                -buf
                chan]} (get @-STORAGE pid)]
    (-must-not-run pid)
    (-set-status! pid ::run)
    (async/go (loop []
                (if (-full? -buf)
                  (-set-error! pid (ex-info
                                     "Buffer overflow/packets will be dropped"
                                     {::type ::buffer-overflow}))
                  (let [msg (async/<! chan)]
                    (try
                      (proc-fn msg)
                      (catch Exception e
                        (-set-error! pid e)))
                    
                    (when (-running? pid) (recur))))))))


(defn -make
  [proc-fn opts]
  (let [buf (-make-buffer opts)]
    {::pid     (-make-pid)
     ::-buf    buf
     ::chan    (async/chan buf)
     ::proc-fn proc-fn
     ::opts    opts
     ::status  nil}))

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


(defn dead?
  [pid]
  (= (-get-status pid) ::error))


(defn send!
  [pid msg]
  (-must-exist pid)
  (when-not (dead? pid)
    (let [chan (->> @-STORAGE (get pid) ::chan)]
      (async/>! chan msg))))


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
