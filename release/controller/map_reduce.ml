open Util
open Worker_manager

(*Hashtables*)
let mhashtbl = Hashtbl.create 0
let combinehash = Hashtbl.create 0 
let rhashtbl = Hashtbl.create 0
(*Locks*)
let hashlock = Mutex.create()
let faillock = Mutex.create()
(*Thread Pools*)
let mthread_pool = Thread_pool.create 100
let rthread_pool = Thread_pool.create 100

(* map kv_pairs map_filename initalizes mappers and iterates over
* kv_pairs, passing work to be attempted by mappers in threads. If the 
* mapper succeeds, it enters the result in a hashtable.  If it fails, its task
* is entered in a list of failed tasks to be attempted again post-iteration.
* Ultimately, information entered in the hashtable is converted into a list.  
*)

let map kv_pairs map_filename : (string * string) list = 
	let workers = (initialize Map map_filename) in 
  let rec mapping kvlist faillst= 
		match kvlist with 
		|h::t -> let worker = (pop_worker workers) in 
      Thread_pool.add_work (fun x ->  
      match (map worker fst(h) snd(h)) with 
      |Some(l) -> 
        mutex.lock hashlock; Hashtbl.add mhashtbl fst(h) l; mutex.unlock hashlock;
      |None -> 
        mutex.lock faillock; (fst(h),snd(h))::faillst; mutex.unlock faillock;
      ) mthread_pool; mapping t
    |[] -> 
      if not (faillst = []) then mapping faillst [] 
      else 
        Thread_pool.destroy mthread_pool; clean_up_workers;
          Hashtbl.fold (fun k v acc -> (k,v)::acc) mhashtbl []
  in mapping kv_pairs []

(* combine kv_pairs combines values with identical keys a tuple 
* consisting of the shared key and a combined list of those values.
* A list is returned where each tuple consists of distinct keys and 
*their associated values.  
*)
let combine kv_pairs : (string * string list) list = 
  List.map (fun elem -> 
    match elem with 
    |key,value -> Hashtbl.add combinehash key value) kv_pairs in
  Hashtbl.fold (fun k v acc -> 
    match acc with 
    |(key,value)::t -> if not(key = k) then (k, [v]) :: acc else (k,v::value)::t
    |[] -> (k,[v])::acc) combinehash []

(* reduce kvs_pairs reduce_filename initalizes reducers and iterates over
* kvs_pairs, passing work to be attempted by reducers in threads. If the 
* reducer succeeds, it enters the result in a hashtable.  If it fails, its task
* is entered in a list of failed tasks to be attempted again post-iteration.
* Ultimately, information entered in the hashtable is converted into a list.  
*)
let reduce kvs_pairs reduce_filename : (string * string list) list =
  let workers = (initialize Reduce reduce_filename) in 
  let rec reducing kvlist faillst= 
    match kvlist with 
    |h::t -> let worker = (pop_worker workers) in 
      Thread_pool.add_work (fun x -> 
        match (reduce worker fst(h) snd(h)) with 
        |Some l -> 
          mutex.lock hashlock;Hashtbl.add rhashtbl fst(h) l;mutex.unlock hashlock; 
        |None -> 
          mutex.lock faillock; (fst(h),snd(h))::faillst; mutex.unlock faillock;
      ) rthread_pool; reducing t 
      |[] -> 
        if not (faillst = []) then (reducing faillst [])
        else 
          Thread_pool.destroy rthread_pool; clean_up_workers;
            Hashtbl.fold(fun k v acc -> (k,v)::acc) rhashtbl []
  in reducing kvs_pairs []

let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced


