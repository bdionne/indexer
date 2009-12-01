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
-export([start_link/0, stop/1, index/1, search/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-import(lists, [map/2]).

-behavior(gen_server).

-include("indexer.hrl").

-record(state, {dbs}).

start_link() -> 
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->    
    {ok, #state{dbs=ets:new(names_pids,[set])}}.

index(DbName) ->
    gen_server:call(?MODULE,{index, DbName}).    

search(DbName, Str) ->
    gen_server:call(?MODULE, {search, DbName, Str}).

stop(DbName) ->
    ?LOG(?INFO, "Scheduling a stop~n", []),
    gen_server:call(?MODULE, {stop, DbName}).



handle_call({index, DbName}, _From, State) ->
    {ok, Pid} = gen_server:start_link(indexer_server, [DbName], []),
    #state{dbs=Tab} = State,
    ets:insert(Tab,{DbName,Pid}),
    spawn_link(fun() -> worker(Pid) end),
    {reply, ok, State};
handle_call({search, DbName, Str}, _From, State) ->
    #state{dbs=Tab} = State,
    [{DbName,Pid}] = ets:lookup(Tab,DbName),
    {reply, indexer_server:search(Pid, Str), State};
handle_call({schedule_stop, DbName}, _From, State) ->
    #state{dbs=Tab} = State,
    [{DbName,Pid}] = ets:lookup(Tab,DbName),
    {reply, indexer_server:schedule_stop(Pid), State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


worker(Pid) ->
    possibly_stop(Pid),
    ?LOG(?DEBUG, "retrieving next batch ~n",[]),
    case indexer_server:next_docs(Pid) of
	{ok, Docs} ->            
	    index_these_docs(Pid, Docs),
	    indexer_server:checkpoint(Pid),
	    possibly_stop(Pid),
            ?LOG(?INFO, "indexed another ~w ~n", [length(Docs)]),
	    %%sleep(5000),
	    worker(Pid);
	done ->
            %% what we need to do here is go into polling mode
            %% and start polling for new updates to the db
	    true
    end.

possibly_stop(Pid) ->
    case indexer_server:should_i_stop(Pid) of
	true ->
	    ?LOG(?INFO, "Stopping~n", []),
	    indexer_server:stop(Pid),
	    exit(stopped);
    	false ->
	    void
    end.

index_these_docs(Pid, Docs) ->
    Ets = indexer_server:ets_table(Pid),
    F1 = fun(Pid1, Doc) -> indexer_words:do_indexing(Pid1, Doc, Ets) end,
    F2 = fun(Key, Val, Acc) -> handle_result(Pid, Key, Val, Acc) end,
    indexer_misc:mapreduce(F1, F2, 0, Docs).

handle_result(Pid, Key, Vals, Acc) ->
    
    indexer_server:write_index(Pid, Key, Vals),
    
    Acc + 1.

sleep(T) ->
    receive
    after T -> true
    end.


