%% ---
%%  Excerpted from "Programming Erlang",
%%  published by The Pragmatic Bookshelf.
%%  Copyrights apply to this code. It may not be used to create training material, 
%%  courses, books, articles, and the like. Contact us if you are in doubt.
%%  We make no guarantees that this code is fit for any purpose. 
%%  Visit http://www.pragmaticprogrammer.com/titles/jaerlang for more book information.
%%
%% Original copyright: (c) 2007 armstrongonsoftware
%%
%% 
%%---
-module(indexer_server).

-export([
         %%cold_start/2, 
	 start/1,
         index/1,
	 next_docs/0,
	 ets_table/0, 
	 checkpoint/0,
	 schedule_stop/0,
	 search/1,
         write_index/2,
	 should_i_stop/0,
	 outdir/0, 
	 stop/0]).

-export([init/1, handle_call/3, handle_cast/2, terminate/2]).
-import(filename, [join/2]).
-include("indexer.hrl").

start(Dir) ->
    ?LOG(?DEBUG, "starting ~p ~p~n", [?MODULE, Dir]),
    gen_server:start({local,?MODULE}, ?MODULE, Dir, []).

schedule_stop() ->
    Check = gen_server:call(?MODULE, schedule_stop),
    case Check of
        ack -> ack;
        %% index is not running go ahead and stop now
        norun -> stop()
    end.

should_i_stop() ->
    gen_server:call(?MODULE, should_i_stop).

stop() ->
     gen_server:cast(?MODULE, stop).


next_docs()   -> gen_server:call(?MODULE, next_docs, infinity).
checkpoint() -> gen_server:call(?MODULE, checkpoint).
outdir()     -> gen_server:call(?MODULE, outdir).
ets_table()  -> gen_server:call(?MODULE, ets_table).
    
search(Str)  -> gen_server:call(?MODULE, {search, Str}).

index(DbName) ->
    gen_server:call(?MODULE, {index, DbName}).

write_index(Key, Vals) ->
    gen_server:call(?MODULE, {write, Key, Vals}).

-record(env,
        {ets, 
         cont, 
         dbnam, 
         idx, 
         nextCP, 
         outdir, 
         dbidxdir, 
         chkp, 
         stop=false}).

init(Dir) ->
    Tab = indexer_trigrams:open(),
    {ok, #env{ets = Tab,
             outdir=Dir}}.    

handle_call({index, DbName}, _From, S) ->
    DbIndexDir = S#env.outdir ++ "/" ++ DbName,
    DbIndexName = list_to_binary(DbName ++ "-idx"),
    case filelib:is_dir(DbIndexDir) of 
        true -> ok;
        false -> file:make_dir(DbIndexDir)
    end,
    case file:list_dir(DbIndexDir) of
	{ok, []} ->
	    Cont = indexer_couchdb_crawler:start(list_to_binary(DbName),[{reset, DbIndexName}]),
	    Check = {DbIndexDir, Cont},
	    ?LOG(?INFO, "creating checkpoint:~p~n", [Check]),
	    indexer_checkpoint:init(DbIndexDir, Check);                 
	_ -> ok
    end,

    {Next, {Dir1, Cont1}} = indexer_checkpoint:resume(DbIndexDir),
    ?LOG(?INFO, "resuming checkpoint:~p~n",[Cont1]),
    {reply, ok, S#env{dbidxdir=Dir1,
                      dbnam=list_to_binary(DbName),
                      idx=DbIndexName,
                      cont=Cont1,
                      nextCP=Next}
    };
handle_call(ets_table, _From, S) ->
    {reply, S#env.ets, S};
handle_call(next_docs, _From, S) ->
    Cont = S#env.cont,
    case indexer_couchdb_crawler:next(Cont) of
	{docs, Docs, ContToCkP} ->
            ?LOG(?DEBUG, "checking the values in next docs ~p ~n",[ContToCkP]),
	    {reply, {ok, Docs}, S#env{chkp=ContToCkP}};
	done ->
	    {reply, done, S}
    end;
handle_call(checkpoint, _From, S) ->
    Next = S#env.nextCP,
    DbIndexDir = S#env.dbidxdir,
    Next1 = indexer_checkpoint:checkpoint(Next, {DbIndexDir, S#env.chkp}),
    S1 = S#env{nextCP = Next1, cont=S#env.chkp},
    {reply, ok, S1};
handle_call(schedule_stop, _From, S) ->
    case S#env.chkp of
       {_, done} -> {reply, norun, S};
        _ -> {reply, ack, S#env{stop=true}}
    end;
handle_call({search, Str}, _From,S) ->
    Result = indexer_misc:search(Str, S#env.ets, S#env.dbnam, S#env.idx),
    {reply, Result, S};

handle_call({write, Key, Vals}, _From,S) ->
    Result = indexer_couchdb_crawler:write_indices(Key, Vals, S#env.idx),
    {reply, Result, S};

handle_call(should_i_stop, _From, S) ->
    {reply, S#env.stop, S};
handle_call(outdir, _From, S) ->
    {reply, S#env.outdir, S}.

handle_cast(stop, S) ->
    {stop, normal, S}.

terminate(Reason, S) ->
    Ets = S#env.ets,
    indexer_trigrams:close(Ets),
    ?LOG(?INFO, "stopping ~p~n",[Reason]).

    



    


