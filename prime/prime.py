from sympy import isprime


def find_primes(num_start: int, num_end: int):
    print("finding primes now")
    if num_start > num_end:
        num_start, num_end = num_end, num_start

    count = 0
    for num in range(num_start, num_end+1):
        if isprime(num):
            count += 1
    print("count is: " + count)
    return count


