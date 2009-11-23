My son, a pianist, was home for the weekend, filling the house with beautiful Shostakovich fugues and Beethoven sonatas, so I decided to relax and hack on the couch.

## FTI for CouchDB databases in CouchDB

Chapter 20 of Joe Armstrong's <a href="http://www.pragprog.com/titles/jaerlang/programming-erlang">Erlang book</a> provides a nice example of the use of processes to do full text indexing with map/reduce. The essential idea is to spawn a process for each document to index and let the reduce function populate the inverted index as it collects the results of the map phase. I recently heard mention of <a href="http://dukesoferl.blogspot.com/2009/07/osmos.html"> in a talk from the NoSQL east conference and it struck me as the ideal data structure for storing an inverted, particularly since it supports user-defined merging. So when one encounters the word Neoplasm in multiple docs one can just write the key/value to the store and let a defined merging function sort things out.

Being the lazy programmer that I am I downloaded the Erlang code sample and modified it a bit to try it out against CouchDB databases, using osmos for the index store. It worked ok until I tried a somewhat larger corpus of data from <a href="http://bitdiddle.cloudant.com:5984/biomedgt/">cancer genomics</a>. Osmos started crashing, I'm sure the issues were minor but I hadn't read that code so I thought why not just store the index in a couch db for now and come back to osmos later. 

It turns out to work better than you'd think. Each distinct word is a document so it does fill space as more documents are processed and each document is updated more and more, but compaction takes it readily back down to a manageable size.

It runs in the same VM with couchdb, using <a href="http://github.com/jchris/hovercraft">hovercraft</a> to interact with couch and provide the docs in <a href="http://github.com/bdionne/indexer/blob/master/indexer_couchdb_crawler.erl">batches</a> to be analyzed.

## Don't try this at home

But if you do it's not too hard. You need a recent copy of hovercraft in your couchdb install directory. For best results install this project in a sibling directory to couchdb. In the top level of indexer:

    mkdir checkpoints

This directory is used <a href="http://github.com/bdionne/indexer/blob/master/indexer.erl#L16">here</a> if you want to move things around. I then typically start couchdb like so:

    ERL_FLAGS='-sname couch@localhost -pa ../indexer' ./utils/run -i

assuming hovercraft is compiled and on the path. The other thing you need is an empty db named foo-idx for each db foo you wish to index.
After starting couch:

    indexer:start().
    indexer:index("biomedgt").
    

And with luck you see these <a href="http://gist.github.com/241278">messages</a>. While it's running you can search:

    <a href="http://gist.github.com/241279">indexer:search(\"Mental Dysfunction\").</a> 

It takes a checkpoint after indexing every n docs, so you can stop() and then call indexer:index("biomedgt") again and it resumes. What gets indexed is all the values in the docs but not the keys or _xxx fields.

 
