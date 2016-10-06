import string
import re
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np
import operator
import csv


def func_input(document):
    """ returns a list of (key = line number, value = line) pairs """
    lines = open(document, "r").readlines()
    regex = re.compile('[%s]' % re.escape(string.punctuation))
    list_pairs = [(index, regex.sub("", line.strip().lower())) for (index, line) in enumerate(lines)]
    return list_pairs


def func_map(key, value):
    """ generates a list of (key = word, value = 1) pairs"""
    words = value.split()
    list_pairs = [(word, 1) for word in words]
    return list_pairs


def func_shuffle_sort(list_nodes):
    """ * collects all the results from the nodes (lines) running the map step
        * groups the pairs (key = word, value = 1) by key
    """
    # flatten the lists of pairs for all lines(nodes)
    flat_list_pairs = [pairs for list_pairs in list_nodes for pairs in list_pairs]
    word_dict = defaultdict(list)
    for word, value in flat_list_pairs:
        word_dict[word].append(value)
    return word_dict


def func_reduce(key, value_list):
    """ generates a list of (key = word, value = count) pairs """
    return key, int(sum(value_list))


def plot_word_counts(list_words_counts, n = 12, figure="word_counts.png"):
    values_plot = [count for word, count in list_words_counts[:n]]
    keys_plot = [word for word, count in list_words_counts[:n]]
    indices = np.arange(n)
    plt.bar(indices, values_plot, align='center')
    plt.xticks(np.arange(n), keys_plot)
    plt.title("Occurrences of the " + str(n) + " most frequent words")
    plt.savefig(figure)


def write_to_csv(list_words_counts, file="word_counts.csv"):
    """ writes the results to a csv file """
    with open(file, 'w') as csvfile:
        fieldnames = ['first_name', 'last_name']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for word, count in list_words_counts:
            writer.writerow({'first_name': word, 'last_name': count})


def word_count_mapReduce(input_file, write_results=True, n_words_plot=12):
    """ simulates a mapReduce word count application for input_file"""
    # input step
    lines = func_input(input_file)
    # map step : each line is processed by a distinct map node
    # create a list of the list of (word, 1) pairs for every node
    map_nodes = [func_map(line_number, line) for line_number, line in lines]
    # shuffle and sort step
    dict_counts = func_shuffle_sort(map_nodes)
    # reduce step : each key is handled by a distinct reduce node
    final_output = [func_reduce(word, counts_list) for word, counts_list in sorted(
                        dict_counts.items(), key=operator.itemgetter(1), reverse=True)]
    for word, count in final_output:
        print('{0:15} {1}'.format(word, count))

    if write_results:
        plot_word_counts(final_output, n_words_plot)
        write_to_csv(final_output)

    return final_output


if __name__ == '__main__':
    input_file = "test_documents/chapter2.txt"
    word_count_mapReduce(input_file)
