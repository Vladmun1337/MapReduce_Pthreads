#include <iostream>
#include <fstream>
#include <algorithm>
#include "pools.h"
#define PATH string("../checker/")
#define ALPHABET_LEN 26

using namespace std;

string parse_string(string s) {
    
    // Convert word to include lowercase only
    string t = "";
    for(int i = 0; i < s.length(); i++)
        if(s[i] >= 'A' && s[i] <= 'Z')
            t += s[i] + 32;
        else if(s[i] >= 'a' && s[i] <= 'z')
            t += s[i];
    
    return t;
}

void *mapper(void *arg) {
    mapper_pool *map_pool = (mapper_pool *)arg;
    string my_file;
    int id;
    bool is_empty;

    // Check if document queue is empty
    pthread_mutex_lock(map_pool->doc_mutex);

    is_empty = (map_pool->docs)->empty();

    pthread_mutex_unlock(map_pool->doc_mutex);
    

    while(!is_empty) {

        pthread_mutex_lock(map_pool->doc_mutex);

        // fetch a document and increase the count
        my_file = (map_pool->docs)->front();
        (map_pool->docs)->pop();

        id = map_pool->file_ids++;

        pthread_mutex_unlock(map_pool->doc_mutex);

        ifstream file_read(PATH + my_file);

        set<word_block, set_cmp> *doc_set = new set<word_block, set_cmp>;
        string word;

        // Read file word by word
        while(file_read >> word) {

            if(parse_string(word) == "")
                continue;

            // Convert word to match format and add to set
            word_block block;
            block.word = parse_string(word);
            block.file_ids.insert(id + 1);

            doc_set->insert(block);
        }

        file_read.close();

        // Check if doc queue is empty
        pthread_mutex_lock(map_pool->doc_mutex);

        is_empty = (map_pool->docs)->empty();

        pthread_mutex_unlock(map_pool->doc_mutex);

        // Add resulting set to set queue
        pthread_mutex_lock(map_pool->set_mutex);

        (map_pool->set_q)->push(doc_set);

       pthread_mutex_unlock(map_pool->set_mutex);

    }

    // Wait at barrier to stop reducers from working
    pthread_barrier_wait(map_pool->bar);
    
    return NULL;
}

void *reducer(void *arg) {

    reducer_pool *reduce_pool = (reducer_pool *)arg;

    // Wait for mappers to finish
    pthread_barrier_wait(reduce_pool->bar);

    set<word_block, set_cmp> *curr_set;
    bool is_empty;

    // Check if set queue is empty
    pthread_mutex_lock(reduce_pool->set_mutex);

    is_empty = (reduce_pool->set_q)->empty();

    pthread_mutex_unlock(reduce_pool->set_mutex);
    

    while(!is_empty) {

        pthread_mutex_lock(reduce_pool->set_mutex);

        // Fetch and parse first set in queue
        curr_set = (reduce_pool->set_q)->front();
        (reduce_pool->set_q)->pop();

        pthread_mutex_unlock(reduce_pool->set_mutex);

        // Add each word to alphabet table
        for(auto &elem : *curr_set) {

            int idx = elem.word[0] - 'a';

            pthread_mutex_lock((reduce_pool->alpha_mutex)[idx]);

            // Search iterator of block with corresponding word
            auto it = find_if((*(reduce_pool->alphabet))[idx].begin(), (*(reduce_pool->alphabet))[idx].end(),
                            [&](const word_block& a) { return a.word == elem.word; });

            // Check if word already exists and sort with set
            if(it == (*(reduce_pool->alphabet))[idx].end()) {
                (*(reduce_pool->alphabet))[idx].insert(elem);

            } else {
                // Get set node from iterator
                auto set_node = (*(reduce_pool->alphabet))[idx].extract(it);

                word_block new_block;
                new_block.word = elem.word;
                new_block.file_ids = set_node.value().file_ids;

                // copy from  current element all indices and add to alphabetical set
                for(auto& id : elem.file_ids)
                    new_block.file_ids.insert(id);

                (*(reduce_pool->alphabet))[idx].insert(new_block);
            }

            pthread_mutex_unlock((reduce_pool->alpha_mutex)[idx]);

        }

        // Free popped set when done parsing
        delete curr_set;

        // Check if set queue is empty
        pthread_mutex_lock(reduce_pool->set_mutex);

        is_empty = (reduce_pool->set_q)->empty();

        pthread_mutex_unlock(reduce_pool->set_mutex);

    }
    
    return NULL;
}

void generate_files(vector<set<word_block, set_cmp>> *alphabet) {

    char let = 'a';

    for(int i = 0; i < ALPHABET_LEN; i++) {

        // Open file of corresponding letter and copy blocks from alphabet sets
        ofstream alpha(string(1, let + i)  + string(".txt"));

        for(auto &it : (*alphabet)[i]) {
            alpha << it.word << ": ";

            auto it1 = it.file_ids.begin();
            alpha << "[" << *it1;
            it1++;

            while(it1 != it.file_ids.end()) {
                alpha << " " << *it1;
                it1++;
            }

            alpha << "]" << endl;
        }

        alpha.close();
    }
}

int main(int argc, char **argv)
{
    if(argc != 4) {
        cerr << "WARNING: argc != 4\n";
        return 1;
    }

    int n;
    string doc;
    queue<string> *docs = new queue<string>;

    ifstream fin(PATH + string(argv[3]));

    // Read documents file and enque titles
    fin >> n;

    for(int i = 0; i < n; i++) {
        fin >> doc;
        docs->push(doc);
    }

    // Build sync and thread dependencies
    pthread_t *mappers_td, *reducers_td;
    pthread_mutex_t *doc_mutex, *set_mutex;
    pthread_barrier_t *bar;

    // Get parameters and convert to int
    int M = atoi(argv[1]), R = atoi(argv[2]);

    mappers_td = new pthread_t[M];
    reducers_td = new pthread_t[R];

    doc_mutex = new pthread_mutex_t;
    set_mutex = new pthread_mutex_t;
    bar = new pthread_barrier_t;

    pthread_mutex_init(doc_mutex, NULL);
    pthread_mutex_init(set_mutex, NULL);
    pthread_barrier_init(bar, NULL, M + R);

    // Build mapper pool
    mapper_pool *map_pool = new mapper_pool;
    queue<set<word_block, set_cmp>*> *set_q = new queue<set<word_block, set_cmp>*>;

    map_pool->docs = docs;
    map_pool->set_q = set_q;
    map_pool->bar = bar;
    map_pool->doc_mutex = doc_mutex;
    map_pool->set_mutex = set_mutex;
    map_pool->file_ids = 0;

    // Build reducer pool
    reducer_pool *reduce_pool = new reducer_pool;

    pthread_mutex_t **alpha_mutex = new pthread_mutex_t*[ALPHABET_LEN];

    for(int i = 0; i < ALPHABET_LEN; i++)
        alpha_mutex[i] = new pthread_mutex_t;

    vector<set<word_block, set_cmp>> alphabet(ALPHABET_LEN);

    reduce_pool->set_q = set_q;
    reduce_pool->bar = bar;
    reduce_pool->set_mutex = set_mutex;
    reduce_pool->alpha_mutex = alpha_mutex;
    reduce_pool->alphabet = &alphabet;

    // Build and join mapper + reducer threads
    for(int i = 0; i < max(M, R); i++) {
        if(i < M)
            pthread_create(&mappers_td[i], NULL, mapper, (void *) map_pool);

        if(i < R)
            pthread_create(&reducers_td[i], NULL, reducer, (void *) reduce_pool);
    }

    for(int i = 0; i < max(M, R); i++) {
        if(i < M)
            pthread_join(mappers_td[i], NULL);

        if(i < R)
            pthread_join(reducers_td[i], NULL);
    }


    // Write pairs to corresponding files
    generate_files(&alphabet);
    
    // Delete pool data 
    delete docs;
    delete set_q;
    delete map_pool;
    delete reduce_pool;

    for(int i = 0; i < ALPHABET_LEN; i++)
        delete alpha_mutex[i];
    
    delete[] alpha_mutex;

    // Destroy sync variables and free memory
    pthread_mutex_destroy(doc_mutex);
    pthread_mutex_destroy(set_mutex);
    pthread_barrier_destroy(bar);

    delete doc_mutex;
    delete set_mutex;
    delete bar;
    delete[] mappers_td;
    delete[] reducers_td;

    return 0;
}
