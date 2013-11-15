open Util;;

let (key, value) = Program.get_input() in
let courses = 
  split_to_class_lst value in
let split = List.fold_left (fun acc x -> (split_spaces x)::acc) [] courses in
Program.set_output (List.fold_left (fun acc x ->
  let tup = 
    match x with 
    |h1::h2::[] -> (h1, h2)
    |_ -> failwith "not possible" in
  tup::acc) [] split)
