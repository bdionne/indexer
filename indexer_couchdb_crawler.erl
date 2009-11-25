%%%-------------------------------------------------------------------
%%% File    : indexer_couchdb_crawler.erl
%%% Author  : Robert Dionne
%%% Description :  hovercraft based crawler over couchdb databases
%%%
%%% Created :  11 Nov 2009 by Robert Dionne <dionne@dionne-associates.com>
%%%
%%% Copyright (C) 2009   Dionne Associates, LLC.
%%%-------------------------------------------------------------------
-module(indexer_couchdb_crawler).
%%
%%
-export([start/2, next/1, lookup_indices/2, write_indices/3]).

-include("../couchdb/src/couchdb/couch_db.hrl").
-include("indexer.hrl").

-define(BATCH_SIZE, 1000).

start(DbName, [{reset, DbIndexName}]) ->
    hovercraft:delete_db(DbIndexName),
    hovercraft:create_db(DbIndexName),    
    {DbName, 0}.

next({DbName, StartId}) ->
    ?LOG(?DEBUG,"getting next for ~p ~p ~n",[DbName, StartId]),
    Docs = case StartId of
               0 -> get_all_docs(DbName, []);
               done -> [];
               _ -> get_all_docs(DbName, [{start_key, StartId}])
           end,           
    case Docs of
        [] -> done;
        {Cont, Docs1} -> {docs, Docs1, {DbName, Cont}}
    end.   

get_all_docs(DbName, Options) ->
    {ok, #db{fd=Fd}} = hovercraft:open_db(DbName),
    {ok, Header} = couch_file:read_header(Fd),
    {ok, IdBtree} = 
        couch_btree:open(Header#db_header.fulldocinfo_by_id_btree_state, Fd,
                         []),    
    {ok, _, Result} = 
        couch_btree:foldl(IdBtree,
                          fun(Key, Acc) ->
                                  ?LOG(?DEBUG, "the key: ~p the acc: ~p ~n",[element(1,Key), Acc]),
                                  case element(1, Acc) of
                                      0 -> {stop, Acc};
                                      _ -> 
                                          {ok, {element(1, Acc) - 1,
                                                [element(1, Key) | element(2, Acc)]}}
                                  end
                          end, {?BATCH_SIZE + 1,[]}, Options),
    Docs = element(2,Result),

    Bool = length(Docs) < ?BATCH_SIZE + 1,

    case length(Docs) of
        0 -> [];
        _ -> ReturnDocs = 
                 lists:map(fun(Id) ->
                                   try
                                       {ok, Doc} = hovercraft:open_doc(DbName, Id),
                                       Doc
                                   catch
                                       _:_ -> []
                                   end                          
                           end, Docs), 
             case Bool of
                 true -> ?LOG(?DEBUG,"ok at the end ~w ~n",[length(ReturnDocs)]),
                         {done, ReturnDocs};
                 _ -> {hd(Docs), lists:reverse(tl(ReturnDocs))}                         
             end 
    end.

lookup_doc(Id, DbName) ->
    try
        hovercraft:open_doc(DbName, Id)
    catch
        _:_ -> not_found
    end.
    

lookup_indices(Word, DbName) ->
    case lookup_doc(list_to_binary(Word), DbName) of
        {ok, Doc} -> proplists:get_value(<<"indices">>,element(1, Doc));
        not_found -> []
    end.

write_indices(Word, Vals, DbName) ->
    %% see if entry already exists
    case lookup_doc(list_to_binary(Word), DbName) of
        {ok, Doc} -> 
            Props = element(1, Doc),
            Indices = proplists:get_value(<<"indices">>, Props),
            ?LOG(?DEBUG,"the current indices ~p ~n",[Indices]),
            NewProps = proplists:delete(<<"indices">>, Props),
            ?LOG(?DEBUG,"props after deleting ~p ~n",[NewProps]),
            NewDoc = 
                {lists:append(NewProps, 
                              [{<<"indices">>,lists:append(Indices, Vals)}] )},
            ?LOG(?DEBUG,"new props ~p ~n",[NewDoc]),
            hovercraft:save_doc(DbName, NewDoc);
        not_found -> 
            NewDoc = {[{<<"_id">>, list_to_binary(Word)},
                       {<<"indices">>,Vals}]},
            hovercraft:save_doc(DbName, NewDoc)
    end.
    
    
    
              





