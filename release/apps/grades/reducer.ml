open Util;;

let (key, values) = Program.get_input() in 
let median = List.fold_left (fun acc x -> 
  let length = List.length values in 
  if (length mod 2) = 0 then 
    let mid1 = length - (length/2)in
    let mid2 = mid1 + 1 in
    let mid1int = 
      int_of_string (List.nth values mid1) in
    let mid2int = 
      int_of_string (List.nth values mid2) in
    let med = (mid1int + mid2int)/2 in 
    string_of_int med
  else 
    let middle = length - ((length - 1)/2) in
    List.nth values middle) "" values in
Program.set_output [median]
