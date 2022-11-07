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
    char *record;
};



//initilize global array to hold key record pairs read from input_file file.
struct key_record **record_array = NULL;
int num_procs = 0;
int n_records = 0;
/*
void merge(int left, int mid, int right){
   
    int i , j , k = 0; 
    int left_size = mid - left + 1;
    int right_size = right - mid;

    struct key_record *array_left[left_size];
    struct key_record *array_right[right_size];

    for (int i = 0; i < left_size; i++)
    {
        array_left[i] = record_array[left + i];
    }
    for (int j = 0; j < right_size; j++)
    {
        array_right[j] = record_array[mid + 1 + j];
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
    int thread_idx = (int) arg + 1;
    int thread_size = record_size / num_procs;
    int bonus = record_size - thread_size * num_procs;
    int left = thread_idx * thread;
    int right = (thread_idx + 1) * ;
    int mid = (left + right) / 2;
    
    if (left < right) {
        merge_sort(left, mid);
        merge_sort(mid + 1, right);
        merge_sort(left, mid, right);
    }
}
*/

int main(int argc, char *argv[]) {
    //num_procs = get_nprocs();

     printf("good, 1 \n");
    //check if more than 2 arguments
    if (argc != 3) {
        write(STDERR_FILENO, error_message, strlen(error_message));
        exit(1);
    }
     printf("good, 2 \n");
    //open input_file
    int input_file = open(argv[1], O_RDONLY);
    if ( input_file == -1) {
        write(STDERR_FILENO, error_message, strlen(error_message));
        exit(1);
    }
     printf("good, 3 \n");
    //open output_file
    int output_file = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (output_file == -1) {
        write(STDERR_FILENO, error_message, strlen(error_message));
        exit(1);
    }
     printf("good, 4 \n");
    //find number of records
    struct stat file_stat;
    if (stat(argv[1], &file_stat) < 0) {
        write(STDERR_FILENO, error_message, strlen(error_message)); 
        exit(1);
    }
    int n_records = (file_stat.st_size / 100);// records split by 100 bytes
    
     printf("good, 5\n");
    //initilaize memory for record_array, and its entries
    record_array = (struct key_record**) calloc(n_records , sizeof(struct key_record));
    for (int i = 0; i < n_records ; i++) {
        record_array[i] = (struct key_record*)malloc(sizeof(struct key_record));
    }
     printf("good, 6 \n");

    //read keys into records_table
    int idx = 0;
     printf("good, 7 \n");
    char *file_ptr = mmap(NULL, file_stat.st_size, PROT_READ, MAP_PRIVATE, input_file, 0);
    for (int i = 0; i < file_stat.st_size; i+=100) {
       
        // cpy first four bytes for the key, 96 for the record into array. 
        memcpy((void*)&record_array[idx]->key, file_ptr+i,4);
         printf("good, 10000 \n");
        record_array[idx]->record = malloc(96);
        memcpy(record_array[idx]->record, file_ptr+i+4, 96);
        printf("good, 20000 \n");
        idx++;
        printf("good, 30000 \n");
    }
    

   
    /* parallel sorting and merge 
    pthread_t threads[num_procs];

    for (int i = 0; i < num_procs; i++) {
        pthread_create(&threads[i], NULL, merge_sort_thread, (void *) i);
    }
    for (int i = 0; i < num_procs; i++) {
        pthread_join(threads[i], NULL);
    }
   */
   
    
    printf("good, 8 \n");
    //write the sorted record_array entries to an output_file 
    for (int i = 0; i < n_records; i++) {
        write(output_file, record_array[i], sizeof(struct key_record));
    }


    //close all open files
    close(input_file);
    close(output_file);
     printf("good, 9 \n");
    //free memory
    for (int i = 0; i < n_records; i++) {
        free(record_array[i]); 
    }
    free(record_array);

     printf("good, 10 \n");
    return 0;
}
