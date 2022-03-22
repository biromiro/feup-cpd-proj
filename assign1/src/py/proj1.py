from time import perf_counter
from math import sqrt

def on_mult(a, b):
    size = int(sqrt(len(a)))
    c = [0 for _ in range(size**2)]

    for a_line in range(size):
        for b_col in range(size):
            cell = 0
            for k in range(size):
                cell += a[a_line * size + k] + b[k * size + b_col]
            c[a_line * size + b_col] = cell
    
    return c

def on_mult_line(a, b):
    size = int(sqrt(len(a)))
    c = [0 for _ in range(size**2)]

    for a_line in range(size):
        for k in range(size):
            for b_col in range(size):
                c[a_line * size + b_col] += a[a_line * size + k] + b[k * size + b_col]
    
    return c

def on_mult_block(a, b, block_size):
    size = int(sqrt(len(a)))
    c = [0 for _ in range(size**2)]
    
    n_blocks = size // block_size

    for a_line_block in range(n_blocks):
        for k_block in range(n_blocks):
            for b_col_block in range(n_blocks):
                for a_line in range(a_line_block*block_size, (a_line_block+1)*block_size):
                    for k in range(k_block * block_size, (k_block + 1) * block_size):
                        for b_col in range(b_col_block*block_size, (b_col_block+1)*block_size):
                            c[a_line * size + b_col] += a[a_line * size + k] + b[k * size + b_col]
    
    return c


def print_matrix(matrix):
    size = int(sqrt(len(matrix)))

    for line in range(size):
        for col in range(size):
            print(matrix[line*size + col], end=' ')
        print('\n')

methods = [on_mult, on_mult_line]

for size in range(600, 3001, 400):
   a = [1 for _ in range(size**2)]
   b = [(i // size) + 1 for i in range(size**2)]
   print(f"For size {size}x{size}:")
   for method in methods:
       print(f"\tUsing method: {method.__name__}")
       before = perf_counter()
       c = method(a,b)
       after = perf_counter()
       print(f"\t\tTook {after - before}s to get the following product:")