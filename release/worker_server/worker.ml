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
		then ()
		else handle_request client
	    |(Some id, "") ->
		Mutex.lock safe;
		Hashtbl.add maptable (Some id) true;
		Mutex.unlock safe;
		if send_response client (Mapper(Some id, ""))
		then ()
		else handle_request client
	    |_ -> failwith "InitMapper: Invalid Compiliation")
        | InitReducer source -> 
            print_endline "Intialize Reducer";
	    (let construct = Program.build source in
	    match construct with 
	    |(None, error) ->
		if (send_response client (Reducer(None, error)))
		then ()
		else handle_request client
	    |(Some id, "") ->
		Mutex.lock safe;
		Hashtbl.add redtable (Some id) true;
		Mutex.unlock safe;
		if send_response client (Reducer(Some id, ""))
		then ()
		else handle_request client
	    |_ -> failwith "InitReducer: Invalid Compiliation")
        | MapRequest (id, k, v) -> 
          print_end_line "Request Mapper";
	    Mutex.lock safe;
	    let found = Hashtbl.mem maptable (Some id) in 
	    Mutex.unlock safe;
	    (match found with 
	    | true -> let result = Program.run id (k,v) in
	      (match result with 
	      | None ->
		  if (send_response client (RuntimeError(id, "MapRequest: None")))
		  then ()
		  else handle_response client
	      | Some n -> 
		  if (send_response client (MapResults(id, n)))
		  then ()
		  else handle_response client)
	    | false -> 
		if (send_response client (InvalidWorker(id))) 
		then ()
		else handle_request client)
        | ReduceRequest (id, k, v) -> 
	    print_end_line "Request Reducer";
            Mutex.lock safe;
	    let found = Hashtbl.mem redtable (Some id) in
	    Mutex.unlock safe;
	    (match found with
	    | true -> let result = Program.run id (k, v) in
	      (match result with
	      | None -> 
		  if (send_response client (RuntimeError(id, "ReduceRequest: None"))) 
		  then ()
		  else handle_request client
	      | Some n -> 
		  if (send_response client (ReduceResults(id, n))) then ()
		  else handle_request client)
	    | false -> 
		if (send_response client (InvalidWorker(id))) then ()
		else handle_request client)
      end
  | None ->
      Connection.close client;
      print_endline "Connection lost while waiting for request."

