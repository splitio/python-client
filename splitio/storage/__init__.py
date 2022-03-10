"""Base storage interfaces."""
import abc


class SplitStorage(object, metaclass=abc.ABCMeta):
    """Split storage interface implemented as an abstract class."""

    @abc.abstractmethod
    def get(self, split_name):
        """
        Retrieve a split.

        :param split_name: Name of the feature to fetch.
        :type split_name: str

        :rtype: str
        """
        pass

    @abc.abstractmethod
    def fetch_many(self, split_names):
        """
        Retrieve splits.

        :param split_names: Names of the features to fetch.
        :type split_names: list(str)

        :rtype: dict
        """
        pass

    @abc.abstractmethod
    def put(self, split):
        """
        Store a split.

        :param split: Split object to store
        :type split_name: splitio.models.splits.Split
        """
        pass

    @abc.abstractmethod
    def remove(self, split_name):
        """
        Remove a split from storage.

        :param split_name: Name of the feature to remove.
        :type split_name: str

        :return: True if the split was found and removed. False otherwise.
        :rtype: bool
        """
        pass

    @abc.abstractmethod
    def get_change_number(self):
        """
        Retrieve latest split change number.

        :rtype: int
        """
        pass

    @abc.abstractmethod
    def set_change_number(self, new_change_number):
        """
        Set the latest change number.

        :param new_change_number: New change number.
        :type new_change_number: int
        """
        pass

    @abc.abstractmethod
    def get_split_names(self):
        """
        Retrieve a list of all split names.

        :return: List of split names.
        :rtype: list(str)
        """
        pass

    @abc.abstractmethod
    def get_all_splits(self):
        """
        Return all the splits.

        :return: List of all the splits.
        :rtype: list
        """
        pass

    @abc.abstractmethod
    def is_valid_traffic_type(self, traffic_type_name):
        """
        Return whether the traffic type exists in at least one split in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        pass

    def get_segment_names(self):
        """
        Return a set of all segments referenced by splits in storage.

        :return: Set of all segment names.
        :rtype: set(string)
        """
        return set([name for spl in self.get_all_splits() for name in spl.get_segment_names()])

    @abc.abstractmethod
    def kill_locally(self, split_name, default_treatment, change_number):
        """
        Local kill for split

        :param split_name: name of the split to perform kill
        :type split_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        pass


class SegmentStorage(object, metaclass=abc.ABCMeta):
    """Segment storage interface implemented as an abstract class."""

    @abc.abstractmethod
    def get(self, segment_name):
        """
        Retrieve a segment.

        :param segment_name: Name of the segment to fetch.
        :type segment_name: str

        :rtype: str
        """
        pass

    @abc.abstractmethod
    def put(self, segment):
        """
        Store a segment.

        :param segment: Segment to store.
        :type segment: splitio.models.segment.Segment
        """
        pass

    @abc.abstractmethod
    def update(self, segment_name, to_add, to_remove, change_number=None):
        """
        Store a split.

        :param segment_name: Name of the segment to update.
        :type segment_name: str
        :param to_add: List of members to add to the segment.
        :type to_add: list
        :param to_remove: List of members to remove from the segment.
        :type to_remove: list
        """
        pass

    @abc.abstractmethod
    def get_change_number(self, segment_name):
        """
        Retrieve latest change number for a segment.

        :param segment_name: Name of the segment.
        :type segment_name: str

        :rtype: int
        """
        pass

    @abc.abstractmethod
    def set_change_number(self, segment_name, new_change_number):
        """
        Set the latest change number.

        :param segment_name: Name of the segment.
        :type segment_name: str
        :param new_change_number: New change number.
        :type new_change_number: int
        """
        pass

    @abc.abstractmethod
    def segment_contains(self, segment_name, key):
        """
        Check whether a specific key belongs to a segment in storage.

        :param segment_name: Name of the segment to search in.
        :type segment_name: str
        :param key: Key to search for.
        :type key: str

        :return: True if the segment contains the key. False otherwise.
        :rtype: bool
        """
        pass


class ImpressionStorage(object, metaclass=abc.ABCMeta):
    """Impressions storage interface."""

    @abc.abstractmethod
    def put(self, impressions):
        """
        Put one or more impressions in storage.

        :param impressions: List of one or more impressions to store.
        :type impressions: list
        """
        pass

    @abc.abstractmethod
    def pop_many(self, count):
        """
        Pop the oldest N impressions from storage.

        :param count: Number of impressions to pop.
        :type count: int
        """
        pass

    @abc.abstractmethod
    def clear(self):
        """
        Clear data.
        """
        pass


class ImpressionPipelinedStorage(object, metaclass=abc.ABCMeta):
    """Impression Pipelined Storage interface."""

    @abc.abstractmethod
    def add_impressions_to_pipe(self, impressions, pipe):
        """
        Add put operation to pipeline

        :param impressions: List of one or more impressions to store.
        :type impressions: list
        :param pipe: Redis pipe.
        :type pipe: redis.pipe
        """
        pass


class EventStorage(object, metaclass=abc.ABCMeta):
    """Events storage interface."""

    @abc.abstractmethod
    def put(self, events):
        """
        Put one or more events in storage.

        :param events: List of one or more events to store.
        :type events: list
        """
        pass

    @abc.abstractmethod
    def pop_many(self, count):
        """
        Pop the oldest N events from storage.

        :param count: Number of events to pop.
        :type count: int
        """
        pass

    @abc.abstractmethod
    def clear(self):
        """
        Clear data.
        """
        pass
