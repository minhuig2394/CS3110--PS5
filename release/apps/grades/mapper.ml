open Util;;

let (key, value) = Program.get_input() in
let courses = 
  List.fold_left (fun acc x ->
	(split_to_class_lst x.course_grades)::acc) [] value in
let split = split_spaces courses in
Program.set_output (List.fold_left (fun acc x ->
  let tup = 
    match x with 
    |h1::h2::[] -> (h1, h2)
    |_ -> failwith "not possible" in
  tup::acc) [] split)
