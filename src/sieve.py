from bitarray import bitarray


class Sieve:
    def __init__(self, limit: int) -> None:
        self.limit: int = limit
        self.primes: bitarray = self._sieve(limit)

    def _sieve(self, limit: int):
        raise NotImplementedError("Subclasses should implement this method.")

    def get_limit(self) -> int:
        return self.limit

    def get_primes(self) -> list[int]:
        return [i for i, is_prime in enumerate(self.primes) if is_prime]

    def get_primes_in_range(self, start: int, end: int) -> list[int]:
        if start > end:
            raise ValueError("Start must be less than or equal to End.")
        return [p for p in self.get_primes() if start <= p <= end]

    def get_nth_prime_in_range(self, start: int, end: int, nth: int) -> int:
        primes_in_range = self.get_primes_in_range(start, end)
        if nth <= 0 or nth > len(primes_in_range):
            raise ValueError(
                "Invalid value for nth. It must be between 1 and the number of primes in the range."
            )
        return primes_in_range[nth - 1]


class SieveOfEratosthenes(Sieve):
    def _sieve(self, limit: int) -> bitarray:
        primes = bitarray(limit + 1)
        primes.setall(True)
        primes[0] = primes[1] = False
        for i in range(2, int(limit**0.5) + 1):
            if primes[i]:
                primes[i * i : limit + 1 : i] = False
        return primes


class SieveOfAtkin(Sieve):
    def _sieve(self, limit: int) -> bitarray:
        is_prime = bitarray(limit + 1)
        is_prime.setall(False)
        sqrt_limit = int(limit**0.5)

        for x in range(1, sqrt_limit + 1):
            for y in range(1, sqrt_limit + 1):
                n = 4 * x * x + y * y
                if n <= limit and (n % 12 == 1 or n % 12 == 5):
                    is_prime[n] = not is_prime[n]

                n = 3 * x * x + y * y
                if n <= limit and n % 12 == 7:
                    is_prime[n] = not is_prime[n]

                n = 3 * x * x - y * y
                if x > y and n <= limit and n % 12 == 11:
                    is_prime[n] = not is_prime[n]

        for n in range(5, sqrt_limit + 1):
            if is_prime[n]:
                for k in range(n * n, limit + 1, n * n):
                    is_prime[k] = False

        primes = bitarray(limit + 1)
        primes.setall(False)
        if limit > 2:
            primes[2] = True
        if limit > 3:
            primes[3] = True
        primes[5:] = is_prime[5:]
        return primes
