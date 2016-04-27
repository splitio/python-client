from __future__ import absolute_import, division, print_function, unicode_literals


from splitio.treatments import CONTROL


class Splitter(object):
    def get_treatment(self, key, seed, partitions):
        if not partitions:
            return CONTROL

        if len(partitions) == 1 and partitions[0].size == 100:
            return partitions[0].treatment

        return self.get_treatment_for_bucket(self.get_bucket(self.hash_key(key, seed)), partitions)

    def hash_key(self, key, seed):
        h = 0

        for c in key:
            h = 31 * h + ord(c)

        return h ^ seed

    def get_bucket(self, key_hash):
        return abs(key_hash % 100) + 1

    def get_treatment_for_bucket(self, bucket, partitions):
        covered_buckets = 0

        for partition in partitions:
            covered_buckets += partition.size

            if covered_buckets >= bucket:
                return partition.treatment

        return CONTROL
