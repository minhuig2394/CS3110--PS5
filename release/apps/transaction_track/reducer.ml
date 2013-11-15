open Util;;

let (key,value) = Program.get_input() in
let present_value = 
  let rec sum amt values= 
    match values with
      |h::t -> let (value:int) = Util.unmarshal h in 
        sum (value+amt) t
      |[] -> amt 
    in let total = sum 0 value
  in 
in Program.set_output (Util.marshal total)