# MAP-REDUCE with PTHREADS

### NOTE: For this project C++17 is required at minimum. The Makefile rule enforces this on build but make sure to have the appropriate version installed!

For this project, a C++ rendition of the **MAP-REDUCE** concurrent paradigm was made, using the *Pthreads* library. The implementation follows creating a list of word and index-list pairs for each word in a set of files. The chosen paradigm optimizes the construction of the list for concurrent systems, splitting the task into 2 types of worker threads:
- Mappers: Apply a function to the input data
- Reducers: Tweak and combine the result of the mappers

## Implementation

The general workflow of the project is as follows:
1. A queue of all selected documents is generated.
2. Every document is dequed and read.
3. For each read document, a set of word - file id pairs is constructed and sorted, later enqued.
4. Each set is parsed, every word extracted is added to a set corresponding to its first letter and overlapping words have file id lists merged.
5. A file for every letter of the alphabet is generated and all the words from the alphabet set list is written, by order of the longest file id list and alphabetical position. This part is done sequentially after all concurrent tasks are finished.

To obtain a balanced complexity of the project, I have taken advantage of the C++ STL with the following data structures:
- **Queue**: to create euqity in reading the files and sets, a queue was used for creating ordinality and efficient push and pop operations.
- **Vector**: Used for the alphabet structure, used efficiently for fast indexing and in-place modifications.
- **Set**: Self-balancing and self-sorting, a comparator was implemented to sort words by file id list length and alphabetical order. Insertion and extraction are fairly balanced. Redundant search if a certain struct member is searched for, but it pays off for the self-sorting property.

## Concurrent approach

To optimize the implementation, the processing of the documents is done in parallel. The number of mappers and reducers is made to scale and has no theoretical limit. Equity and efficiency was key when splitting the task to the two types of workers.

**NOTE**: A reducer thread is NOT allowed to work before all mapper threads ar finished. To ensure this synchronization, a pthread barrier is used for all threads: at the end of the task for mappers and before the task for reducers.

### Mappers

Each mapper deques a file from the document queue provided by the main thread. To synchronize the reading correctly, a **mutex** was used for popping and checking if the queue is empty.

Once dequed, the file is opened and read word by word. To keep consistency in the search, all non alphabetical characters were removed and the remaining were converted to lowercase.

For every parsed file, a set is constructed that keeps a structure named *word_block* (a word string and an integer set for the file id's). To keep the same workflow for the next type of workers, the sets are added to a queue set, to be treated as documents in a queue, although for this task synchronization is used for writing to the set queue.

To appropriately syncrhonize, read and write, the mappers contain a structure named **mapper pool** that holds metadata, such as: mutexes, barrier, document queue and final set queue. Some of these are reused in the reducer pool to properly communicate between tasks.

### Reducers

After the barrier is opened by the last mappers, the reducers start the work. For the first part of the task, they operate the same as the mappers and deque a set.

Once a set is fetched, every word is read and searched in the alphabet set vector. If it doesn't appear, we will simply insert it. If it's found, the original instance is extracted, file id sets are merged, obtaining a set of sorted integer values. The final value is added back to the set. To avoid duplicated instances in the set, a mutex is provided for the selected letter of the alphabet, in order to prioritize the reading and writing to the first occuring thread.

With this approach, the result will be a vector with every position being a letter of the alphabet, and its value a set of **word_block** type. Thus, every word is categorized by its first letter, sorted by file id apparitions and alphabetical order and unified to a central data structure.

The reducers' pool contains the set queue, the alphabetical vector, an array of mutexes for each letter of the alphabet and a mutex for the set queue.

## Conclusions

This project was very fun and gave me the opportunity to further my understandings in optimization through parallelism. For every new worker thread added, a considerate speedup is added.

To test for yourself, I encourage running the project on the given dataset with the `./run_with_docker.sh` command.