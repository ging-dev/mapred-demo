import re

from mrjob.job import MRJob

class AverageWordLength(MRJob):
    def mapper(self, _, line: str):
        for word in re.findall(r'\w+', line):
            yield word, 1

    def reducer(self, key, values: list[int]):
        yield key, sum(values)


if __name__ == '__main__':
    mr_job = AverageWordLength(args=['example.txt'])
    with mr_job.make_runner() as runner:
        runner.run()
        words_num = total_len = 0
        for key, value in mr_job.parse_output(runner.cat_output()):
            key: str
            value: int

            print(f'{key} - {value}')
            words_num += value
            total_len += value * len(key)

        print(f'Result: {round(total_len/words_num, 2)}')
