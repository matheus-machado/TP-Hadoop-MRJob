from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWordLength(MRJob):

    def mapper(self, _, line):
        words = line.split()
        for word in words:
            word = word.lower()
            vowels = "aeiouy"
            flag_vowel = 0
            found_vowel = False
            for char in word:
                if char in vowels:
                    flag_vowel = 1
                    if not found_vowel:
                        found_vowel = True
                        vowel = char
                    elif char != vowel:
                        flag_vowel = -1
                        break
            if flag_vowel == 1:
                yield (vowel, (len(word), word))

    def reducer(self, key, values):
        max_word = max(values)
        yield (key, max_word)


if __name__ == '__main__':
    MRWordLength.run()
