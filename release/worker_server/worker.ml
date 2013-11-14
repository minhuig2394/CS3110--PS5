open Protocol

let maptable = Hashtbl.create 200
let redtable = Hashtbl.create 200
let safe = Mutex.create ()

let send_response client response =
  let success = Connection.output client response in
    (if not success then
      (Connection.close client;
       print_endline "Connection lost before response could be sent.")
    else ());
    success

let rec handle_request client =
  match Connection.input client with
    Some v ->
      begin
        match v with
        | InitMapper source -> 
	    print_endline "Intialize Mapper";
	    (let construct = Program.build source in
	    match construct with 
	    |(None, error) ->
		if (send_response client (Mapper(None, error)))
		then handle_request client
		else ()
	    |(Some id, "") ->
		Mutex.lock safe;
		Hashtbl.add maptable (Some id) true;
		Mutex.unlock safe;
		if (send_response client (Mapper(Some id, "")))
		then handle_request client
		else ()
	    |_ -> failwith "InitMapper: Invalid Compiliation")
        | InitReducer source -> 
            print_endline "Intialize Reducer";
	    (let construct = Program.build source in
	    match construct with 
	    |(None, error) ->
		if (send_response client (Reducer(None, error)))
		then handle_request client
		else ()
	    |(Some id, "") ->
		Mutex.lock safe;
		Hashtbl.add redtable (Some id) true;
		Mutex.unlock safe;
		if send_response client (Reducer(Some id, ""))
		then handle_request client
		else ()
	    |_ -> failwith "InitReducer: Invalid Compiliation")
        | MapRequest (id, k, v) -> 
          print_endline "Request Mapper";
	    Mutex.lock safe;
	    let found = Hashtbl.mem maptable (Some id) in 
	    Mutex.unlock safe;
	    (match found with 
	    | true -> let result = Program.run id (k,v) in
	      (match result with 
	      | None ->
		  if (send_response client (RuntimeError(id, "MapRequest: None")))
		  then handle_request client
		  else ()
	      | Some n -> 
		  if (send_response client (MapResults(id, n)))
		  then handle_request client
		  else ())
	    | false -> 
		if (send_response client (InvalidWorker(id))) 
		then handle_request client
		else ())
        | ReduceRequest (id, k, v) -> 
	    print_endline "Request Reducer";
            Mutex.lock safe;
	    let found = Hashtbl.mem redtable (Some id) in
	    Mutex.unlock safe;
	    (match found with
	    | true -> let result = Program.run id (k, v) in
	      (match result with
	      | None -> 
		  if (send_response client (RuntimeError(id, "ReduceRequest: None"))) 
		  then handle_request client
		  else ()
	      | Some n -> 
		  if (send_response client (ReduceResults(id, n))) then handle_request client
		  else ())
	    | false -> 
		if (send_response client (InvalidWorker(id))) then handle_request client
		else ())
      end
  | None ->
      Connection.close client;
      print_endline "Connection lost while waiting for request."



