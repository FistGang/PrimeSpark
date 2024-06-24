
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
python3 prime_generator.py range <start: default to 1> <end> <output_dir> --num_slices <num: default to 16> --sieve <sieve_method>
```
For example, to find prime numbers between 10 and 100 using the Sieve of Eratosthenes and save the results to the `primes` directory:

```bash
python3 prime_generator.py range 10 100 --sieve eratosthenes primes
```

To use the Sieve of Atkin:

```bash
python3 prime_generator.py range 10 100 --sieve atkin primes
```

#### Finding the nth Prime in a Range

To find the nth prime number within a specified range, use the following command:

```bash
python3 prime_generator.py nth <start: default to 1> <end> --nth <nth> --sieve <sieve_method> --num_slices <num: default to 16> <output_dir>
```

For example, to find the 5th prime number between 10 and 100 using the Sieve of Eratosthenes and save the result to the `primes` directory:

```bash
python3 prime_generator.py nth 10 100 --nth 5 --sieve eratosthenes primes
```

To use the Sieve of Atkin:

```bash
python3 prime_generator.py nth 10 100 --nth 5 --sieve atkin primes
```

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any improvements or bug fixes.

