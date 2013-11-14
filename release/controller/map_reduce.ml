open Util
open Worker_manager

(*Hashtables*)
let mhashtbl = Hashtbl.create 1
let mtasktbl = Hashtbl.create 1
let combinehash = Hashtbl.create 1 
let rhashtbl = Hashtbl.create 1
let rtasktbl = Hashtbl.create 1
(*Locks*)
let hashlock = Mutex.create()
let tasklock = Mutex.create()

(*Thread Pools*)
let mthread_pool = Thread_pool.create 20
let rthread_pool = Thread_pool.create 20

(* map kv_pairs map_filename initalizes mappers and iterates over
* kv_pairs, passing work to be attempted by mappers in threads. If the 
* mapper succeeds, it enters the result in a hashtable. 
* Ultimately, information entered in the hashtable is converted into a list.  
*)
let map kv_pairs map_filename : (string * string) list = 
  print_endline "map1";
  List.iter
    (fun elem -> 
      Hashtbl.add mtasktbl elem false) kv_pairs;
  print_endline "map2";
  let workers = (initialize_mappers map_filename) in 
    (while Hashtbl.length mtasktbl > 0 do 
      print_endline "task length is ";
      print_int (Hashtbl.length mtasktbl);
        print_endline "whileloop";
      Hashtbl.iter (fun k v -> 
      print_endline "work";
      (match k with 
        |(key,value) ->
          (Thread_pool.add_work (fun x ->
            let worker = (pop_worker workers) in 
            print_endline "123";
            match (map worker key value) with 
            |Some(l) -> 
              print_endline "345";
              (Mutex.lock tasklock); 
              if Hashtbl.mem mtasktbl k then 
                ((Hashtbl.remove mtasktbl k);
                  (Mutex.lock hashlock); 
                  (List.iter (
                    fun elem -> Hashtbl.add mhashtbl (fst elem) (snd elem)) l); 
                  (Mutex.unlock hashlock)
                  )
              else (); 
              (Mutex.unlock tasklock);push_worker workers worker
            |None -> ()
            ) mthread_pool))
      ) mtasktbl; 
      Thread.delay 0.1;
    done);
    print_endline "cleanup";
  clean_up_workers workers; 
  Thread_pool.destroy mthread_pool; 
    print_endline "kvs";
  Hashtbl.fold (fun k v acc -> (k,v)::acc) mhashtbl []


(* combine kv_pairs combines values with identical keys a tuple 
* consisting of the shared key and a combined list of those values.
* A list is returned where each tuple consists of distinct keys and 
*their associated values.  
*)
let combine kv_pairs : (string * string list) list =
  print_endline "combining"; 
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
  print_endline "reducing";
  List.iter
    (fun elem -> 
      Hashtbl.add rtasktbl elem false) kvs_pairs;
  let workers = (initialize_reducers reduce_filename) in 
    while Hashtbl.length rtasktbl > 0 do 
      Hashtbl.iter (fun k v -> 
      match k with 
        |(key,value) ->
          (Thread_pool.add_work (fun x ->
            let worker = (pop_worker workers) in 
            match (reduce worker key value) with 
            |Some(l) -> 
              (Mutex.lock tasklock); 
              if Hashtbl.mem rtasktbl k then 
                ((Hashtbl.remove rtasktbl k);
                  (Mutex.lock hashlock); 
                    Hashtbl.add rhashtbl key l; 
                  (Mutex.unlock hashlock)
                  )
              else (); 
              (Mutex.unlock tasklock);push_worker workers worker
            |None -> ()
            ) rthread_pool)) rtasktbl; 
      Thread.delay 0.1;
    done;
  clean_up_workers workers; 
  Thread_pool.destroy rthread_pool; 
  print_endline "end reducing";
  Hashtbl.fold (fun k v acc -> (k,v)::acc) rhashtbl []

let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced


