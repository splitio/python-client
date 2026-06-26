import abc

from splitio_commons.storage.inmemmory import InMemoryDefinitionStorage, InMemoryDefinitionStorageAsync

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
    def update(self, to_add, to_delete, new_change_number):
        """
        Update feature flag storage.

        :param to_add: List of feature flags to add
        :type to_add: list[splitio.models.splits.Split]
        :param to_delete: List of feature flags to delete
        :type to_delete: list[splitio.models.splits.Split]
        :param new_change_number: New change number.
        :type new_change_number: int
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


class InMemorySplitStorage(InMemoryDefinitionStorage):
    
    def __init__(self, internal_event_queue, flag_sets=[]):
        """Constructor."""
        InMemoryDefinitionStorage.__init__(self, internal_event_queue, flag_sets)
        
    def get_splits_count(self):
        return self.get_definitions_count()
    
    def get_all_splits(self):
        return self.get_all_definitions()

    def get_split_names(self):
        return self.get_definition_names()

    def get_feature_flags_by_sets(self, sets):
        return self.get_definitions_by_sets(sets)

class InMemorySplitStorageAsync(InMemoryDefinitionStorageAsync):
    
    def __init__(self, internal_event_queue, flag_sets=[]):
        """Constructor."""
        InMemoryDefinitionStorageAsync.__init__(self, internal_event_queue, flag_sets)
        
    async def get_splits_count(self):
        return await self.get_definitions_count()
    
    async def get_all_splits(self):
        return await self.get_all_definitions()

    async def get_split_names(self):
        return await self.get_definition_names()

    async def get_feature_flags_by_sets(self, sets):
        return await self.get_definitions_by_sets(sets)
