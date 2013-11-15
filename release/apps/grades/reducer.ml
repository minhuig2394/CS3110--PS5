open Util;;

let (key, values) = Program.get_input() in 
let com v1 v2 = 
  if float_of_string v1 > float_of_string v2 then 1
  else if float_of_string v1 < float_of_string v2 then -1
  else 0 in 
let sortedval = 
  List.sort com values in
let median =
  let length = List.length sortedval in 
  if (length mod 2) = 0 then 
    let mid1 = length - (length/2) - 1in
    let mid2 = mid1 in
    let mid1int = 
      float_of_string (List.nth sortedval mid1) in
    let mid2int = 
      float_of_string (List.nth sortedval mid2) in
    let med = (mid1int +. mid2int) /. 2. in 
    string_of_float med
  else 
    let middle = length - ((length - 1)/2) - 1 in
    List.nth sortedval middle in
Program.set_output [median]
