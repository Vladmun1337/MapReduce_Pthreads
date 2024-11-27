#include <iostream>
#include <fstream>
#include <algorithm>
#include "pools.h"
#define PATH string("../checker/")
#define ALPHABET_LEN 26

using namespace std;

string parse_string(string s) {

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

    pthread_mutex_lock(map_pool->doc_mutex);

    is_empty = (map_pool->docs)->empty();

    pthread_mutex_unlock(map_pool->doc_mutex);
    

    while(!is_empty) {

        pthread_mutex_lock(map_pool->doc_mutex);

        my_file = (map_pool->docs)->front();
        (map_pool->docs)->pop();

        id = map_pool->file_ids++;

        pthread_mutex_unlock(map_pool->doc_mutex);

        ifstream file_read(PATH + my_file);

        // TODO free sets
        set<word_block, set_cmp> *doc_set = new set<word_block, set_cmp>;
        string word;

        while(file_read >> word) {

            if(parse_string(word) == "")
                continue;

            word_block block;
            block.word = parse_string(word);
            block.file_ids.insert(id + 1);

            // make comparator for set
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

    pthread_barrier_wait(map_pool->bar);
    
    return NULL;
}

void *reducer(void *arg) {

    

    reducer_pool *reduce_pool = (reducer_pool *)arg;

    pthread_barrier_wait(reduce_pool->bar);

    set<word_block, set_cmp> *curr_set;
    bool is_empty;

    pthread_mutex_lock(reduce_pool->set_mutex);

    is_empty = (reduce_pool->set_q)->empty();

    pthread_mutex_unlock(reduce_pool->set_mutex);
    

    while(!is_empty) {

        pthread_mutex_lock(reduce_pool->set_mutex);

        // TODO free set
        curr_set = (reduce_pool->set_q)->front();
        (reduce_pool->set_q)->pop();

        pthread_mutex_unlock(reduce_pool->set_mutex);

        // Add to alphabet table

        for(auto &elem : *curr_set) {

            int idx = elem.word[0] - 'a';

            pthread_mutex_lock((reduce_pool->alpha_mutex)[idx]);

            auto it = find_if((*(reduce_pool->alphabet))[idx].begin(), (*(reduce_pool->alphabet))[idx].end(),
                            [&](const word_block& a) {return a.word == elem.word; });

            // Check if word already exists and sort with set
            if(it == (*(reduce_pool->alphabet))[idx].end()) {
                (*(reduce_pool->alphabet))[idx].insert(elem);

            } else {
                // Get node from set iterator
                auto set_node = (*(reduce_pool->alphabet))[idx].extract(it);

                word_block new_block;
                new_block.word = elem.word;
                new_block.file_ids = set_node.value().file_ids;

                // copy from element all indices and add to alphabetical set
                for(auto& id : elem.file_ids)
                    new_block.file_ids.insert(id);

                (*(reduce_pool->alphabet))[idx].insert(new_block);

            }

            pthread_mutex_unlock((reduce_pool->alpha_mutex)[idx]);

        }

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
    ofstream alpha(string(1, let)  + string(".txt"));

    for(int i = 0; i < ALPHABET_LEN; i++) {

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
        cout << "Doc " << i << " is " << doc << endl;

        docs->push(doc);
    }

    // Build structs for sync

    pthread_mutex_t *doc_mutex, *set_mutex;
    pthread_barrier_t *bar;
    int M = atoi(argv[1]), R = atoi(argv[2]);

    pthread_t *mappers_td, *reducers_td;

    mappers_td = (pthread_t*) malloc(M * sizeof(pthread_t));
    reducers_td = (pthread_t*) malloc(R * sizeof(pthread_t));

    doc_mutex = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
    set_mutex = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t));
    bar = (pthread_barrier_t*) malloc(sizeof(pthread_barrier_t));

    pthread_mutex_init(doc_mutex, NULL);
    pthread_mutex_init(set_mutex, NULL);
    pthread_barrier_init(bar, NULL, M + R);

    // structs for map

    mapper_pool *map_pool = new mapper_pool;
    queue<set<word_block, set_cmp>*> *set_q = new queue<set<word_block, set_cmp>*>;

    map_pool->docs = docs;
    map_pool->set_q = set_q;
    map_pool->bar = bar;
    map_pool->doc_mutex = doc_mutex;
    map_pool->set_mutex = set_mutex;
    map_pool->file_ids = 0;

    // structs for reduce

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

    cout << "set queue len is " << map_pool->set_q->size() << " before" << endl;

    // Build threads

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


    cout << "set queue len is " << map_pool->set_q->size() << " after" << endl; 


    // Check set queue out + free (TODO: delete without prints for sanity)
    generate_files(&alphabet);

    
    // Release and exit
    delete docs;
    delete set_q;
    delete map_pool;
    delete reduce_pool;
    //delete alphabet;

    for(int i = 0; i < ALPHABET_LEN; i++)
        delete alpha_mutex[i];
    
    delete[] alpha_mutex;

    pthread_mutex_destroy(doc_mutex);
    pthread_mutex_destroy(set_mutex);
    pthread_barrier_destroy(bar);

    free(doc_mutex);
    free(set_mutex);
    free(bar);
    free(mappers_td);
    free(reducers_td);

    return 0;
}