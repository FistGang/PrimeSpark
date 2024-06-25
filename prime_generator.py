from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD
import os
import argparse
from src.sieve import Sieve, SieveOfEratosthenes, SieveOfAtkin

NUM_SLICES = 16


def get_sieve_class(sieve_name: str) -> type[Sieve]:
    if sieve_name == "eratosthenes":
        return SieveOfEratosthenes
    elif sieve_name == "atkin":
        return SieveOfAtkin
    else:
        raise ValueError(f"Unknown sieve method: {sieve_name}")


def get_primes_in_range(
    sc: SparkContext,
    start: int,
    end: int,
    sieve_class: type[Sieve],
    partitions=NUM_SLICES,
) -> RDD[int]:
    if start > end:
        raise ValueError("Start must be less than or equal to End.")

    broadcast_end = sc.broadcast(end)

    sieve = sieve_class(broadcast_end.value)
    primes_up_to_end = sieve.get_primes()

    primes_rdd = sc.parallelize(primes_up_to_end, numSlices=partitions)
    primes_in_range = primes_rdd.filter(lambda x: start <= x <= end)

    return primes_in_range


def generate_primes_in_range(
    sc: SparkContext,
    start: int,
    end: int,
    output_dir: str,
    sieve_class: type[Sieve],
    partitions=NUM_SLICES,
) -> None:
    try:
        primes_in_range = get_primes_in_range(sc, start, end, sieve_class, partitions)

        output_file = f"output_primes_{start}_{end}"
        output_path = os.path.join(output_dir, output_file)

        primes_in_range.map(str).saveAsTextFile(output_path)

        print(
            f"There are {primes_in_range.count()} prime numbers in range {start} to {end}, written to {output_path}"
        )

    except Exception as e:
        print(f"Error: {e}")


def get_nth_prime_in_range(
    sc: SparkContext,
    start: int,
    end: int,
    nth: int,
    output_dir: str,
    sieve_class: type[Sieve],
    partitions=NUM_SLICES,
) -> None:
    try:
        primes_in_range = get_primes_in_range(sc, start, end, sieve_class, partitions)

        if nth <= 0 or nth > primes_in_range.count():
            raise ValueError(
                "Invalid value for nth. It must be between 1 and the number of primes in the range."
            )
        nth_prime = primes_in_range.take(nth)[-1]

        nth_prime_file = f"nth_prime_{nth}_from_{start}_to_{end}"
        nth_prime_path = os.path.join(output_dir, nth_prime_file)

        sc.parallelize([str(nth_prime)]).saveAsTextFile(nth_prime_path)

        print(
            f"There are {primes_in_range.count()} prime numbers in range {start} to {end}. The {nth} prime number is: {nth_prime}, written to {nth_prime_path}"
        )

    except Exception as e:
        print(f"Error: {e}")


def main(
    method: str,
    start: int,
    end: int,
    nth: int,
    output_dir: str,
    sieve: str,
    partitions=NUM_SLICES,
) -> None:
    conf = SparkConf().setAppName("PrimeNumberGenerator")
    sc = SparkContext(conf=conf)

    sieve_class = get_sieve_class(sieve)

    try:
        if method == "range":
            generate_primes_in_range(
                sc, start, end, output_dir, sieve_class, partitions
            )
        elif method == "nth":
            if nth is None:
                raise ValueError("Parameter nth must be provided for method 'nth'")
            get_nth_prime_in_range(
                sc, start, end, nth, output_dir, sieve_class, partitions
            )
        else:
            raise ValueError(
                "Invalid method. Use 'range' to generate primes in a range or 'nth' to get the nth prime number."
            )
    finally:
        sc.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate prime numbers within a range or find the nth prime number using PySpark."
    )
    parser.add_argument(
        "method",
        type=str,
        choices=["range", "nth"],
        help="Method to use: 'range' or 'nth'",
    )
    parser.add_argument(
        "start",
        type=int,
        nargs="?",
        default=1,
        help="Start of the range (inclusive), default is 1",
    )
    parser.add_argument("end", type=int, help="End of the range (inclusive)")
    parser.add_argument(
        "--nth",
        type=int,
        help="Find the nth prime number in the range (only for 'nth' method)",
        default=None,
    )
    parser.add_argument("output_dir", help="Output directory for storing results")
    parser.add_argument(
        "--sieve",
        type=str,
        choices=["eratosthenes", "atkin"],
        required=True,
        help="Sieve method to use: 'eratosthenes' or 'atkin'",
    )
    parser.add_argument(
        "--num_slices",
        type=int,
        default=16,
        help="Number of slices for parallelizing the RDD, default is 16",
    )
    args = parser.parse_args()

    main(
        args.method,
        args.start,
        args.end,
        args.nth,
        args.output_dir,
        args.sieve,
        args.num_slices,
    )
