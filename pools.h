#include <queue>
#include <set>
#include <string>
#include <pthread.h>


struct word_block {
    std::string word;
    std::set<int> file_ids;
};

struct set_cmp {
    // Comparator for set
    bool operator()(const word_block& a, const word_block& b) const {
        if(a.word[0] == b.word[0]) {
            if(a.file_ids.size() == b.file_ids.size())
                return a.word < b.word;

            return a.file_ids.size() > b.file_ids.size();
        }
        
        return a.word[0] < b.word[0];
    }
};

struct mapper_pool {
    std::queue<std::string> *docs;
    std::queue<std::set<word_block, set_cmp>*> *set_q;
    pthread_mutex_t *doc_mutex, *set_mutex;
    pthread_barrier_t *bar;
    int file_ids;
};

struct reducer_pool {
    std::queue<std::set<word_block, set_cmp>*> *set_q;
    std::vector<std::set<word_block, set_cmp>> *alphabet;
    pthread_mutex_t *set_mutex;
    pthread_mutex_t **alpha_mutex;
    pthread_barrier_t *bar;
};
