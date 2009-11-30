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

-include("indexer.hrl").

start() -> ok.

index(DbName) ->
    indexer_server:start(DbName),
    spawn_link(fun() -> worker() end).
    

search(Str) ->
    indexer_server:search(Str).

stop() ->
    ?LOG(?INFO, "Scheduling a stop~n", []),
    indexer_server:schedule_stop().

worker() ->
    possibly_stop(),
    ?LOG(?DEBUG, "retrieving next batch ~n",[]),
    case indexer_server:next_docs() of
	{ok, Docs} ->            
	    index_these_docs(Docs),
	    indexer_server:checkpoint(),
	    possibly_stop(),
            ?LOG(?INFO, "indexed another ~w ~n", [length(Docs)]),
	    %%sleep(5000),
	    worker();
	done ->
            %% what we need to do here is go into polling mode
            %% and start polling for new updates to the db
	    true
    end.

possibly_stop() ->
    case indexer_server:should_i_stop() of
	true ->
	    ?LOG(?INFO, "Stopping~n", []),
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


