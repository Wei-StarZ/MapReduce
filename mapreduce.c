#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"

Partitioner partition_;
Mapper map_;
Reducer reduce_;
int **recorder;
int argc_;
char **argv_;
int counter;
int currentFile;
int num_partitions_;
typedef struct {
    char *key;
    char *value;
} Node;

typedef struct {
    int capacity;
    int size;
    Node *nodes;
    pthread_mutex_t lock;
} list;
int t;
list **table;
pthread_mutex_t filelock = PTHREAD_MUTEX_INITIALIZER;
int reducerCount;
int currentP;
pthread_mutex_t reducelock = PTHREAD_MUTEX_INITIALIZER;

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {
    if (num_partitions == 1) {
        return 0;
    }
    unsigned int receiver = atoi(key);
    int count = 0;
    while (num_partitions != 1) {
        num_partitions /= 2;
        ++count;
    }
    unsigned int highbit = receiver >> (32 - count);
    return highbit;
}

void MR_Emit(char *key, char *value) {
    int index = partition_(key, num_partitions_);
    if (strcmp(key, "\0")) {
        pthread_mutex_lock(&table[index]->lock);
        Node node;
        node.key = strdup(key);
        node.value = strdup(value);
        if (table[index]->size >= table[index]->capacity) {
            table[index]->capacity *= 2;
            table[index]->nodes = realloc(table[index]->nodes,
                    table[index]->capacity * sizeof(Node));
        }
        table[index]->nodes[table[index]->size] = node;
        ++table[index]->size;
        pthread_mutex_unlock(&table[index]->lock);
    }
}

void *map_thread(void *arg) {
    while (currentFile != argc_) {
        pthread_mutex_lock(&filelock);
        if (currentFile >= argc_) {
            pthread_mutex_unlock(&filelock);
            break;
        }
        char *filename = argv_[currentFile++];
        pthread_mutex_unlock(&filelock);
        map_(filename);
    }
    return NULL;
}
//This comparator is from the Geeksforgeek, the oracle of qsort
int comparator(const void *p, const void *q) {
    Node a = *(Node *) p;
    Node b = *(Node *) q;
    return strcmp(a.key, b.key);
}

void *sort_thread(void *arg) {
    int index = *(int *) arg;
    free(arg);
    Node *nodes = table[index]->nodes;
    qsort((void *) nodes, table[index]->size, sizeof(Node), comparator);
    return NULL;
}

char *get_next(char *key, int partition_num) {
    if (*recorder[partition_num] >= table[partition_num]->size) {
        return NULL;
    }
    Node *nodes = table[partition_num]->nodes;
    int *record = recorder[partition_num];
    if (strcmp(key, nodes[*record].key) == 0) {
        return nodes[(*record)++].value;
    }
    return NULL;
}

void *reduce_thread(void *arg) {
    int partition_number;
    while (currentP < num_partitions_) {
        pthread_mutex_lock(&reducelock);
        partition_number = currentP++;
        pthread_mutex_unlock(&reducelock);
        if (partition_number > num_partitions_) {
            break;
        }
        int *record = recorder[partition_number];
        Node *nodes = table[partition_number]->nodes;
        int size = table[partition_number]->size;
        *record = 0;
        while (*record < size) {
            char *key = nodes[*record].key;
            reduce_(key, get_next, partition_number);
        }
    }
    return NULL;
}

void
MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce,
        int num_reducers, Partitioner partition, int num_partitions) {
    argc_ = argc;
    argv_ = argv;
    partition_ = partition;
    map_ = map;
    reduce_ = reduce;
    currentFile = 1;
    num_partitions_ = num_partitions;
    counter = 1;
    currentP = 0;
    reducerCount = 0;
    int i = 0;
    int j = 0;
    t = 1;
    table = malloc(num_partitions_ * sizeof(list *));
    for (i = 0; i < num_partitions_; ++i) {
        table[i] = malloc(sizeof(list));
        int rc = pthread_mutex_init(&table[i]->lock, NULL);
        assert(!rc);
        table[i]->capacity = 1024;
        table[i]->size = 0;
        table[i]->nodes = malloc(table[i]->capacity * sizeof(Node));
    }
    recorder = malloc(num_partitions_ * sizeof(int *));
    for (i = 0; i < num_partitions; ++i) {
        recorder[i] = malloc(sizeof(int));
        *recorder[i] = 0;
    }
    pthread_t mapping[num_mappers];
    i = 0;
    for (i = 0; i < num_mappers; ++i) {
        pthread_create(&mapping[i], NULL, map_thread, argv_);
    }
    for (j = 0; j < i; ++j) {
        pthread_join(mapping[j], NULL);
    }
    pthread_t sorting[num_partitions];
    for (i = 0; i < num_partitions; ++i) {
        int *index = malloc(sizeof(int));
        *index = i;
        pthread_create(&sorting[i], NULL, sort_thread, index);
    }
    for (i = 0; i < num_partitions; ++i) {
        pthread_join(sorting[i], NULL);
    }
    pthread_t reducing[num_reducers];
    for (i = 0; i < num_reducers; ++i) {
        pthread_create(&reducing[i], NULL, reduce_thread, NULL);
    }

    for (i = 0; i < num_reducers; ++i) {
        pthread_join(reducing[i], NULL);
    }

    for (i = 0; i < num_partitions_; ++i) {
        for (j = 0; j < table[i]->size; ++j) {
            free(table[i]->nodes[j].key);
            free(table[i]->nodes[j].value);
        }
        free(recorder[i]);
        recorder[i] = NULL;
        free(table[i]->nodes);
        table[i]->nodes = NULL;
        free(table[i]);
        table[i] = NULL;
    }
    free(recorder);
    recorder = NULL;
    free(table);
    table = NULL;
}
