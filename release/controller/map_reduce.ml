open Util
open Worker_manager

(*Hashtables*)
let mhashtbl = Hashtbl.create 0
let mtasktbl = Hashtbl.create 0
let combinehash = Hashtbl.create 0 
let rhashtbl = Hashtbl.create 0
let rtasktbl = Hashtbl.create 0
(*Locks*)
let hashlock = Mutex.create()
let tasklock = Mutex.create()

(*Thread Pools*)
let mthread_pool = Thread_pool.create 100
let rthread_pool = Thread_pool.create 100

(* map kv_pairs map_filename initalizes mappers and iterates over
* kv_pairs, passing work to be attempted by mappers in threads. If the 
* mapper succeeds, it enters the result in a hashtable. 
* Ultimately, information entered in the hashtable is converted into a list.  
*)
let map kv_pairs map_filename : (string * string) list = 
  let add = List.fold_left
    (fun acc elem -> 
      Hashtbl.add mtasktbl (acc,elem) false;(acc+1)) 0 kv_pairs in ();
  let workers = (initialize_mappers map_filename) in 
  let rec mapping b=
    (if b = false then 
      (Mutex.lock tasklock;
      let finished = 
      Hashtbl.fold (fun k v acc -> 
        let worker = (pop_worker workers) in 
        match k with 
        |i,(key,value) ->
          if v = false then 
          (Thread_pool.add_work (fun x ->
            match (map worker key value) with 
            |Some(l) -> 
              (Mutex.lock tasklock); 
              if (Hashtbl.find mtasktbl k )= false then 
                ((Hashtbl.replace mtasktbl k true);
                  (Mutex.lock hashlock); 
                  (List.iter (fun elem -> Hashtbl.add mhashtbl (fst elem) (snd elem)) l); 
                  (Mutex.unlock hashlock))
              else (); 
              (Mutex.unlock tasklock)
            |None -> ()
            ) mthread_pool; false) 
          else acc) mtasktbl true in (); 
           Mutex.unlock tasklock;
       mapping finished)
    else Thread_pool.destroy mthread_pool; clean_up_workers workers)
      in mapping false; Hashtbl.fold (fun k v acc -> (k,v)::acc) mhashtbl []


(* combine kv_pairs combines values with identical keys a tuple 
* consisting of the shared key and a combined list of those values.
* A list is returned where each tuple consists of distinct keys and 
*their associated values.  
*)
let combine kv_pairs : (string * string list) list = 
  List.iter (fun elem -> 
    match elem with 
    |key,value -> Hashtbl.add combinehash key value) kv_pairs;
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
  let add = List.fold_left
    (fun acc elem -> 
      Hashtbl.add rtasktbl (acc,elem) false;(acc+1)) 0 kvs_pairs in ();
  let workers = (initialize_reducers reduce_filename) in 
  let rec reducing b=
    if b = false then 
      (Mutex.lock tasklock;
      let finished = 
        Hashtbl.fold (fun k v acc -> 
          let worker = (pop_worker workers) in 
         match k with 
         |i,(key,value) ->
            if v = false then 
            (Thread_pool.add_work (fun x ->
              match (reduce worker key value) with 
              |Some(l) -> 
               Mutex.lock tasklock; 
               if Hashtbl.find rtasktbl k = false then 
                 (Hashtbl.replace rtasktbl k true;
                 Mutex.lock hashlock; 
                  Hashtbl.add rhashtbl key l;
                 Mutex.unlock hashlock)
               else (); Mutex.unlock tasklock
             |None -> ()
             ) rthread_pool; false)
             else acc) rtasktbl true in (); Mutex.unlock tasklock;
        reducing finished)
    else Thread_pool.destroy rthread_pool; clean_up_workers workers
    in reducing false;
      Hashtbl.fold (fun k v acc -> (k,v)::acc) rhashtbl []

let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced


