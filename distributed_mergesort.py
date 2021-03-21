import time
import math
import random
import argparse
from multiprocessing import Pool


def merge(numbers, l1, r1, l2, r2):
    sorted_num = [0] * (r2 - l1 + 1) 
    i = 0
    while (l1 <= r1 or l2 <= r2):
        if l1 > r1:
            sorted_num[i] = numbers[l2]
            l2 += 1
        elif l2 > r2:
            sorted_num[i] = numbers[l1]
            l1 += 1
        elif numbers[l1] < numbers[l2]:
            sorted_num[i] = numbers[l1]
            l1 += 1
        else:
            sorted_num[i] = numbers[l2]
            l2 += 1
        i += 1
    return sorted_num


def mergesort(numbers, l, r):
    if l == r:
        return

    m = (l + r) // 2

    mergesort(numbers, l, m)
    mergesort(numbers, m+1, r)

    numbers[l:r+1] = merge(numbers, l, m, m+1, r)
    return


def parallel_mergesort(numbers):
    mergesort(numbers, 0, len(numbers)-1)
    return numbers


def merge_lists(v1, v2):
    return merge(v1 + v2, 0, len(v1)-1, len(v1), len(v1)+len(v2)-1)


def parallel_merge(v):
    m = len(v) // 2 - 1
    return merge(v, 0, m, m + 1, len(v)-1)


if __name__ == "__main__":
    started = time.time()

    parser = argparse.ArgumentParser(description='Sort a random list of integers')
    parser.add_argument('-n', '--size', type=int, help='number of random integers to be sorted')
    parser.add_argument('-c', '--cores', type=int, help='number of cores to be used')

    args = parser.parse_args()

    n_size = args.size
    n_cores = args.cores

    #Create a list of random integers
    values = random.sample(range(1, n_size + 5), n_size)

    chunk_size = math.ceil(len(values) / n_cores)

    # Separate input into array of arrays
    chunks = [values[i:i + chunk_size] for i in range(0, len(values), chunk_size)]

    # Start the parallel process
    pool = Pool(n_cores)
    chunks = pool.map(parallel_mergesort, chunks)

    # chunks is an array of arrays containing the returned sorted chunk of each processor.
    # Here we concatenate each two arrays and merge them parallel. Repeat this process 
    # until only 2 arrays are left.
    while(len(chunks) > 2):
        data=[]
        i = 0
        while (i < len(chunks)):
            if i + 1 < len(chunks):
                data += [chunks[i] + chunks[i+1]]   
            else:
                data += [chunks[i]]
            i += 2
        chunks = pool.map(parallel_merge, data)
    
    # We have to merge them separately because the sizes might be different and it that case
    # parallel_merge will not work.
    if (len(chunks) == 2):
        final = merge_lists(chunks[0], chunks[1])
    else:
        final = chunks[0]
    
    #print(final)
    elapsed = time.time()
    print('Time taken MultiProcess :', elapsed - started)

    pool.close()
