%% ---
%%  Excerpted from "Programming Erlang",
%%  published by The Pragmatic Bookshelf.
%%  Copyrights apply to this code. It may not be used to create training material, 
%%  courses, books, articles, and the like. Contact us if you are in doubt.
%%  We make no guarantees that this code is fit for any purpose. 
%%  Visit http://www.pragmaticprogrammer.com/titles/jaerlang for more book information.
%%
%%  Original copyright: "(c) 2007 armstrongonsoftware"
%%---
-module(indexer).
-export([start/0, stop/0, index/1, search/1]).

-import(lists, [map/2]).

output_dir()    -> "/Users/bitdiddle/emacs/indexer/checkpoints".

start() ->
    indexer_server:start(output_dir()).

index(DbName) ->
    indexer_server:index(DbName),
    spawn_link(fun() -> worker() end).

search(Str) ->
    indexer_server:search(Str).

stop() ->
    io:format("Scheduling a stop~n"),
    indexer_server:schedule_stop().

worker() ->
    possibly_stop(),
    case indexer_server:next_docs() of
	{ok, Docs} ->            
	    index_these_docs(Docs),
	    indexer_server:checkpoint(),
	    possibly_stop(),
            io:format("indexed another ~w ~n",[length(Docs)]),
	    sleep(5000),
	    worker();
	done ->
	    true
    end.

possibly_stop() ->
    case indexer_server:should_i_stop() of
	true ->
	    io:format("Stopping~n"),
	    indexer_server:stop(),
	    exit(stopped);
    	false ->
	    void
    end.

index_these_docs(Docs) ->
    Ets = indexer_server:ets_table(),
    F1 = fun(Pid, Doc) -> indexer_words:do_indexing(Pid, Doc, Ets) end,
    F2 = fun(Key, Val, Acc) -> handle_result(Key, Val, Acc) end,
    indexer_misc:mapreduce(F1, F2, 0, Docs).

handle_result(Key, Vals, Acc) ->
    
    indexer_server:write_index(Key, Vals),
    
    Acc + 1.

sleep(T) ->
    receive
    after T -> true
    end.
