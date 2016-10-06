import string
import re
from collections import defaultdict
from itertools import zip_longest
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
    """
        * generates a list of (key = word, value = 1) pairs
        * TODO: for larger amounts of data, it would have been better to write the map results into the standard output
        instead of passing them directly ( in case it doesn't fit in memory)
    """
    words = value.split()
    list_pairs = [(word, 1) for word in words]
    return list_pairs


def split_dict_(input_dict, number_chunks):
    """ splits dictionary by key into equal chunks """

    # prep with empty dicts
    dict_list = [dict() for i in range(number_chunks)]
    i = 0
    for key, value in input_dict.items():
        dict_list[i][key] = value
        if i < number_chunks-1:
            i += 1
        else:
            i = 0
    return dict_list


def func_shuffle_sort(list_nodes, n_reduce_nodes=10):
    """
        * collects all the results from the nodes (lines) running the map step
        * groups the pairs (key = word, value = 1) by key
        * distribute the keys among the nodes for the reduce step
    """
    # flatten the lists of pairs for all lines(nodes)
    flat_list_pairs = [pairs for list_pairs in list_nodes for pairs in list_pairs]
    word_dict = defaultdict(list)
    for word, value in flat_list_pairs:
        word_dict[word].append(value)
    # divide the keys into n_reduce_nodes chunks, but the processing of each key is independent
    return split_dict_(word_dict, n_reduce_nodes)


def func_reduce(key, value_list):
    """ generates a pair of (key = word, list (value = count)) """
    result = list()
    result.append(int(sum(value_list)))
    return key, result


def plot_word_counts(list_words_counts, n=12, figure="word_counts.png"):
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
        fieldnames = ['word', 'counts']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for word, count in list_words_counts:
            writer.writerow({'word': word, 'counts': count})


def word_count_mapReduce(input_file, n_reduce_nodes=10, write_results=True, n_words_plot=12):
    """ simulates a mapReduce word count application for input_file"""
    # input reader step
    lines = func_input(input_file)

    # map step : each line is processed by a distinct map node
    # create a list of the list of (word, 1) pairs for every node
    map_nodes = [func_map(line_number, line) for line_number, line in lines]

    # shuffle and sort step
    dict_reduce_nodes = func_shuffle_sort(map_nodes)

    # reduce step: each chunk of map results is handled by a reduce node, then we merge the results of each node
    final_output = list()
    for dict_counts in dict_reduce_nodes:
        final_output += [func_reduce(word, counts_list) for word, counts_list in dict_counts.items()]

    final_output = [(word, count[0]) for word, count in sorted(
                           final_output, key=operator.itemgetter(1), reverse=True)]
    # output writer step
    for word, count in final_output:
        print('{0:15} {1}'.format(word, count))
    if write_results:
        plot_word_counts(final_output, n_words_plot)
        write_to_csv(final_output)

    return final_output


if __name__ == '__main__':
    input_file = "test_documents/chapter1.txt"
    number_reduce_nodes = 10
    word_count_mapReduce(input_file=input_file, n_reduce_nodes=number_reduce_nodes)
