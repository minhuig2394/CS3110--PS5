open Util;;

let main (args : string array) : unit =
  if Array.length args < 3 then
    print_endline "Usage: grades <filename>"
  else
   let filename = args.(2) in
    let students = load_grades filename in 
    let courses = 
      List.fold_left (fun acc x ->
	(split_to_class_lst x.course_grades)::acc) [] students in
    let reduced = 
      Map_reduce.map_reduce "grades" "mapper" "reducer" courses in
    print_reduced_courses reduced

in

main Sys.argv
