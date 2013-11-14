open Util;;

let (key, value) = Program.get_input() in
let split = split_spaces value in
Program.set_output (List.fold_left (fun acc x ->
  let tup = 
    match x with 
    |h1::h2::[] -> (h1, h2)
    |_ -> failwith "not possible" in
  tup::acc) [] split)
