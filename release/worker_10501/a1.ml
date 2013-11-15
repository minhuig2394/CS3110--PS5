open Util;;

let format blk data = 
  match data with 
  |h::t -> let (txct:int) = (Util.unmarshal h) in 
    let rec transaction txct t reslst= 
      if txct > 0 then begin
        match t with 
        |inct::outct::t -> 
        let (incount:int) = (Util.unmarshal inct) in 
        let rec inids n translst reslst= 
          if n > 0 then begin
          match translst with 
          |h::t -> 
            let newlst = (h, 0)::reslst
            in inids (n - 1) t newlst
          |_ -> failwith "invalid block"
        end
          else translst,reslst
      in let trans,results = inids incount t reslst in 
    let rec outids n translst results =
      if n > 0 then begin
        match translst with 
        |oid::oamt::t -> 
          let (outamt:int) = (Util.unmarshal oamt) in 
         let newlst = (oid,outamt)::results in 
            outids (n - 1) t newlst
        |_ -> failwith "invalid block"
      end 
      else translst,results
    in let (ocount:int) = (Util.unmarshal outct) 
    in let transactions,kvs = outids ocount trans results 
  in transaction (txct - 1) transactions kvs
  |_ -> failwith "invalid bock"
end 
else reslst
in transaction txct t []
|_ -> failwith "invalid block"
 in
let combine lst = 
    let (_,kvs) = List.fold_left (fun acc elem  -> 
  match acc,elem with 
  |(amt,newlst),(key,value) -> 
    if not(value = 0) then 
      (value+amt),(key,(Util.marshal value))::newlst 
    else 
      (amt),(key, (Util.marshal (value-amt)))::newlst)
    (0,[]) lst 
  in kvs 
in
let key,value = Program.get_input() in 
let to_combine = format (Util.split_words value) key in 
  Program.set_output(combine (to_combine))
