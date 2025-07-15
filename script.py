from database import dbconnection

from utils import TextClear, TagsCounting
from hash import HashingTags


# Select the desired project (int)  interest to you

PROJECT_ID = 1

projects = dbconnection.get_projects(PROJECT_ID)


def tags_tree_builder():
    hashing_tags = HashingTags()
    # Selecting publications from the selected project


    for proj_id in projects:
        publications = dbconnection.get_publications(*proj_id)

        for publication_id, content in publications:

            tags_counting_and_sorting, clearing = TagsCounting(), TextClear()
            # Removing punctuation
            text_clear = clearing.removal_punctuation_marks(content.lower())
            # Text splitting
            list_words = text_clear.split()
            # Removing unnecessary parts of speech
            list_word_without_garbage = clearing.removing_words(list_words)
            # Normalizing a word
            normalize_words = clearing.normalize_form(list_word_without_garbage)
            # "Count of repetitions of tags in the list"
            tags_with_count = tags_counting_and_sorting.counter(normalize_words)
            # Merging tags from different publications
            non_sorted_lst = tags_counting_and_sorting.sum_counter(tags_with_count, publication_id)
            # Adding tags to hash
            hashing_tags.insert_hash(*proj_id, non_sorted_lst)

    # Unpacking tags from hash for successful insertion into the database
    tags_to_insert = hashing_tags.unpacking_tags_from_hash_to_insert()

    # Inserting tags in the DB
    dbconnection.insert_new_tags(tags_to_insert)


if __name__ == "__main__":
    tags_tree_builder()
