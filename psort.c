#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <sys/stat.h>
#include <sys/mman.h>

char error_message[30] = "An error has occurred\n";

//struct to hold key value pairs from input_file file, (first four bytes are the key)
struct key_record {
    int key;
    int record[24];
};

//initilize global array to hold key record pairs read from input_file file.
struct key_record **record_array = NULL;
int num_procs = 0;
int n_records = 0;

void print_records() {
    for (int i = 0; i < n_records; i++) {
        printf("key: %d\n", record_array[i]->key);
    }
}

// variation on geeks for geeks: https://www.geeksforgeeks.org/merge-sort-using-multi-threading/
void merge(int left, int mid, int right){
    int left_size = mid - left + 1;
    int right_size = right - mid;

    struct key_record *left_array[left_size];
    struct key_record *right_array[right_size];

    int n_left = mid - left + 1;
    int n_right = right - mid;

    for (int i = 0; i < n_left; i++) {
        left_array[i] = record_array[left + i];
    }
    for (int i = 0; i < n_right; i++) {
        right_array[i] = record_array[mid + 1 + i];
    }

    int k = left;
    int i = 0, j =0;

    while (i < n_left && j < n_right) {
        if (left_array[i]->key <= right_array[j]->key) {
            record_array[k++] = left_array[i++];
        } else {
            record_array[k++] = right_array[j++];
        }
    }

    while (i < n_left) {
        record_array[k++] = left_array[i++];
    }

    while (j < n_right) {
        record_array[k++] = right_array[j++];
    }
}

void merge_sort(int left, int right) {
    if (left < right) {
        int mid = (left + right) / 2; 
        merge_sort(left, mid); 
        merge_sort(mid + 1, right); 
        merge(left, mid, right);
    }
}

void* merge_sort_thread(void *arg) {
    int thread_idx = (int) arg;
    int thread_size = n_records / num_procs;
    int bonus = n_records - thread_size * num_procs;
    int left = thread_idx * thread_size;
    int right = (thread_idx + 1) * thread_size - 1;

    // put the remaining workload on the last thread, though inefficient!
    if (bonus > 0 && thread_idx == num_procs - 1) {
        right += bonus;
    }

    int mid = (left + right) / 2;
    
    if (left < right) {
        merge_sort(left, mid);
        merge_sort(mid + 1, right);
        merge(left, mid, right);
    }
}


int main(int argc, char *argv[]) {
    num_procs = get_nprocs();

    //check if more than 2 arguments
    if (argc != 3) {
        write(STDERR_FILENO, error_message, strlen(error_message));
        exit(1);
    }
    
    //open input_file
    int input_file = open(argv[1], O_RDONLY);
    if ( input_file == -1) {
        write(STDERR_FILENO, error_message, strlen(error_message));
        exit(1);
    }
    //open output_file
    int output_file = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (output_file == -1) {
        write(STDERR_FILENO, error_message, strlen(error_message));
        exit(1);
    }

    //find number of records
    struct stat file_stat;
    if (stat(argv[1], &file_stat) < 0) {
        write(STDERR_FILENO, error_message, strlen(error_message)); 
        exit(1);
    }

    n_records = (file_stat.st_size / 100);// records split by 100 bytes

    //initilaize memory for record_array, and its entries
    record_array = (struct key_record**) malloc(n_records * sizeof(struct key_record *));
    for (int i = 0; i < n_records ; i++) {
        record_array[i] = (struct key_record *) malloc(sizeof(struct key_record));
    }

    //read keys into records_table
    // printf("file size %d n_records: %d \n", file_stat.st_size, n_records);
    int idx = 0;
    char *file_ptr = mmap(NULL, file_stat.st_size, PROT_READ, MAP_PRIVATE, input_file, 0);
    for (int i = 0; i < file_stat.st_size; i+=100) {
       
        // cpy first four bytes for the key, 96 for the record into array. 
        memcpy((void*)&record_array[idx]->key, file_ptr+i, 4);
        //printf("good, 10000, %d \n", i);
        memcpy(record_array[idx]->record, file_ptr+i+4, 96);
        //printf("good, 20000, %d \n", i);
        idx++;
        //printf("good, 30000 \n");
    }

    //print_records();
    
    pthread_t threads[num_procs];

    for (int i = 0; i < num_procs; i++) {
        pthread_create(&threads[i], NULL, merge_sort_thread, (void *) i);
    }
    for (int i = 0; i < num_procs; i++) {
        pthread_join(threads[i], NULL);
    }
 
    // merge
    int thread_size = n_records / num_procs;
    int bonus = n_records - thread_size * num_procs;
 
    for (int i = 0; i < num_procs - 1; i++) {
        int left = 0;
        int mid = (i + 1) * thread_size - 1;
        int right = (i + 2) * thread_size - 1;

        if (i == num_procs - 2) {
            right += bonus;
        }
        //  printf("%d %d %d\n", left, mid, right);        
        merge(left, mid, right);
    }
    
    //print_records();


    //printf("good, 8 \n");
    //write the sorted record_array entries to an output_file 
    for (int i = 0; i < n_records; i++) {
        write(output_file, record_array[i], sizeof(struct key_record));
    }

    //close all open files
    close(input_file);
    close(output_file);
    //free memory
    for (int i = 0; i < n_records; i++) {
        free(record_array[i]); 
    }
    free(record_array);

    return 0;
}

