import json

class HashingTags:

    def __init__(self):
        self.all_hash = {}
    # Adding tags from a post to a hash
    def insert_hash(self, proj_id, tags):
        if self.all_hash.get(proj_id) is None:
            self.all_hash[proj_id] = dict(tags)
        else:
            for k, v in dict(tags).items():
                if self.all_hash[proj_id].get(k) is None:
                    self.all_hash[proj_id][k] = [v[0], v[1]]
                else:
                    self.all_hash[proj_id][k][0] += v[0]
                    if self.all_hash[proj_id][k][1] != v[1]:
                        self.all_hash[proj_id][k][1] += (*v[1],)

        return self.all_hash

    def sorting_tags_in_hash(self, key):
        return sorted(self.all_hash[key].items(), key=lambda tpl: tpl[1], reverse=True)

    def unpacking_tags_from_hash_to_insert(self):
        """Unpacking tags for insertion into the database"""
        unpack_tag_lst = []

        for key in self.all_hash.keys():
            temp = (key,)
            tag_with_values = tuple(self.sorting_tags_in_hash(key))
            for x in range(len(tag_with_values)):
                temp +=(tag_with_values[x][0],)
                temp += (tag_with_values[x][1][0],)
                temp += (json.dumps(tag_with_values[x][1][1]),)


            unpack_tag_lst.append(temp)

        for row in unpack_tag_lst:
            print(row)
        return unpack_tag_lst

