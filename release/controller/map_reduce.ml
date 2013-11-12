open Util
open Worker_manager

let mhashtbl = Hashtbl.create 0
let combinehash = Hashtbl.create 0 
let rhashtbl = Hashtbl.create 0

let hashlock = Mutex.create()
let faillock = Mutex.create()

let mthread_pool = Thread_pool.create 100
let rthread_pool = Thread_pool.create 100
(* TODO implement these *)
let map kv_pairs map_filename : (string * string) list = 
	let workers = (initialize Map map_filename) in 
  let rec mapping kvlist faillst= 
		match kvlist with 
		|h::t -> let worker = (pop_worker workers) in 
      Thread_pool.add_work (fun x ->  
      match (map worker fst(h) snd(h)) with 
      |Some(l) -> 
        mutex.lock hashlock; Hashtbl.add mhashtbl fst(h) l; mutex.unlock hashlock;
      |None -> mutex.lock faillock; (fst(h),snd(h))::faillst; mutex.unlock faillock;
      ) mthread_pool; mapping t
    |[] -> 
      if not (faillst = []) then mapping faillst [] 
      else 
        Thread_pool.destroy mthread_pool; Hashtbl.fold (fun k v acc -> (k,v)::acc) mhashtbl []
  in mapping kv_pairs []

let combine kv_pairs : (string * string list) list = 
  List.map (fun elem -> 
    match elem with 
    |key,value -> Hashtbl.add combinehash key value) kv_pairs in
  Hashtbl.fold (fun k v acc -> 
    match acc with 
    |(key,value)::t -> if not(key = k) then (k, [v]) :: acc else (k,v::value)::t
    |[] -> (k,[v])::acc) combinehash []

let reduce kvs_pairs reduce_filename : (string * string list) list =
  let workers = (initialize Reduce map_filename) in 
  let rec reducing kvlist faillst= 
    match kvlist with 
    |h::t -> let worker = (pop_worker workers) in 
      Thread_pool.add_work (fun x -> 
        match (reduce worker fst(h) snd(h)) with 
        |Some l -> 
          mutex.lock hashlock; Hashtbl.add rhashtbl fst(h) l ; mutex.unlock hashlock; 
        |None -> mutex.lock faillock; (fst(h),snd(h))::faillst; mutex.unlock faillock;
      ) rthread_pool; reducing t 
      |[] -> 
        if not (faillst = []) then (reducing faillst [])
        else 
          Thread_pool.destroy rthread_pool; Hashtbl.fold(fun k v acc -> (k,v)::acc) rhashtbl []


let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced


