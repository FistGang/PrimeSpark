
## Prime Number Generator using PySpark

This project is a Prime Number Generator that uses PySpark to calculate prime numbers within a given range or to find the nth prime number within a specified range. The program uses sieves algorithm to generate prime numbers up to a specified limit and then filters the prime numbers within the desired range. The results are saved to an output directory specified by the user.

## Prerequisites

Before running this project, ensure you have the following installed:

- Python 3.9+
- Apache Spark
- PySpark

## Usage

## Installations

You need to install dependencies to run the project that specified in the `requirements.txt` as follow:

```bash
# Create virtual environment
python3 -m venv .venv
# Activate environment
source .venv/bin/activate
# Install deps
python3 -m pip install -r requirements.txt
```

### Running the Script

The script supports two modes of operation: generating primes within a range and finding the nth prime within a range.

#### Generating Primes in a Range

To generate prime numbers within a specified range, use the following command:

```bash
python3 prime_generator.py --method range --start <lower limit: default to 1> --end <upper limit> --output_dir <path/to/your/output/dir> --num_slices <num slice> --sieve <sieve name>
```
For example, to find prime numbers between 10 and 100 using the Sieve of Eratosthenes and save the results to the `primes` directory:

```bash
python3 prime_generator.py --method range --start 10 --end 100 --numslice 16 --output_dir primes --sieve eratosthenes
```

To use the Sieve of Atkin:

```bash
python3 prime_generator.py --method range --start 10 --end 100 --numslice 16 --output_dir primes --sieve atkin
```

#### Finding the nth Prime in a Range

To find the nth prime number within a specified range, use the following command:

```bash
python3 prime_generator.py --method nth --start <lower limit: default to 1> --end <upper limit> --output_dir <path/to/your/output/dir> --num_slices <num slice> --sieve <sieve name>
```

For example, to find the 5th prime number between 10 and 100 using the Sieve of Eratosthenes and save the result to the `primes` directory:

```bash
python3 prime_generator.py --method nth --start 10 --end 100 --nth 5 --numslice 16 --output_dir primes --sieve eratosthenes
```

To use the Sieve of Atkin:

```bash
python3 prime_generator.py --method nth --start 10 --end 100 --nth 5 --numslice 16 --output_dir primes --sieve atkin
```

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any improvements or bug fixes.

