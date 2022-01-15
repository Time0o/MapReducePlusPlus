# MapReduce++

MapReduce++ is an implemention of the
[MapReduce](https://en.wikipedia.org/wiki/MapReduce) programming model written
in C++ and based on gRPC.

## Installation

In order to build MapReduce++ you will need to have Protobuf, gRPC and spglog
already installed on your system. You may then run:

```
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release \
         -DMR_IMPL=../mr/word_count.cc
         -DMR_REDUCE_NUM_TASKS=3 \
```

Here, `MR_IMPL` must point to the file that contains the actual map and reduce
functions. Take a look at `word_count.cc` and note that the former is a coroutine
that yields words tokenized from a stringstream. `MR_REDUCE_NUM_TASKS` is the
number of reduce tasks that should be created (note that the number of map tasks
is equal to the number of input files as we will see in the next section).

## Usage

For the wordcount task, given a number of input files `1.txt`, `2.txt` etc., we
can set up the master by running `master 1.txt 2.txt ...`. Each worker that is
subsequently started (without arguments) will then automatically begin
receiving tasks from the master. When all tasks are done, master and workers
will automatically terminate.

Let's look at an example: the `run_demo.sh` script starts a master on a number
of long text documents (books) and then starts three workers. The endproducts
produced by these workers are files of the form `demo/out/reduce_out_*.mr`
which when concatenated together and subsequently sorted form the output: a
list of all words occurring in any of the books alongside their counts found in
`demo/out/reduce_out.mr`.
