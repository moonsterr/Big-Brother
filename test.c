#include <stdio.h>

int main() {
    // 1. RECTANGULAR (A solid block of 6 slots: 2 rows, 3 cols)
    int rect[2][3] = { {10, 11, 12}, {20, 21, 22} };

    // 2. POINTER ARRAY (3 separate arrays, and 1 array of their addresses)
    int row0[] = {100, 101, 102};
    int row1[] = {200, 201, 202};
    int *ptr_arr[2] = { row0, row1 };

    printf("--- 1. RECTANGULAR ARRAY (The 'Warehouse') ---\n");
    printf("Base address (rect):        %p\n", (void*)rect);
    printf("Address of rect[0][0]:     %p (Value: %d)\n", (void*)&rect[0][0], rect[0][0]);
    printf("Address of rect[1][0]:     %p (Value: %d)\n", (void*)&rect[1][0], rect[1][0]);
    printf("Note: Distance is exactly %lu bytes (3 ints * 4 bytes).\n\n", 
            (char*)&rect[1][0] - (char*)&rect[0][0]);

    printf("--- 2. POINTER ARRAY (The 'Directory') ---\n");
    printf("Base address of ptr_arr:    %p\n", (void*)ptr_arr);
    printf("Value stored in ptr_arr[0]: %p (Points to row0)\n", (void*)ptr_arr[0]);
    printf("Value stored in ptr_arr[1]: %p (Points to row1)\n", (void*)ptr_arr[1]);
    printf("Address of ptr_arr[1][0]:   %p (Value: %d)\n", (void*)&ptr_arr[1][0], ptr_arr[1][0]);
    
    return 0;
}