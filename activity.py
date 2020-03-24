import logging
from typing import Optional, Dict, Any, List

from hdx.data.hdxobject import HDXObject, HDXObjectUpperBound
from hdx.hdx_configuration import Configuration

logger = logging.getLogger(__name__)


class Activity(HDXObject):
    def __init__(self, initial_data=None, configuration=None):
        # type: (Optional[Dict], Optional[Configuration]) -> None
        if not initial_data:
            initial_data = dict()
        super(Activity, self).__init__(initial_data, configuration=configuration)

    @staticmethod
    def actions():
        # type: () -> Dict[str, str]
        """Dictionary of actions that can be performed on object

        Returns:
            Dict[str, str]: Dictionary of actions that can be performed on object
        """
        return {
            'package list': 'package_activity_list'
        }

    @staticmethod
    def read_from_hdx(id_field, configuration=None):
        # type: (str, Optional[Configuration]) -> Optional[HDXObjectUpperBound]
        """Abstract method to read the HDX object given by identifier from HDX and return it

        Args:
            id_field (str): HDX object identifier
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[T <= HDXObject]: HDX object if successful read, None if not
        """
        raise NotImplementedError

    def check_required_fields(self, ignore_fields=list()):
        # type: (List[str]) -> None
        """Abstract method to check that metadata for HDX object is complete. The parameter ignore_fields should
        be set if required to any fields that should be ignored for the particular operation.

        Args:
            ignore_fields (List[str]): Fields to ignore. Default is [].

        Returns:
            None
        """
        raise NotImplementedError

    def update_in_hdx(self):
        # type: () -> None
        """Abstract method to check if HDX object exists in HDX and if so, update it

        Returns:
            None
        """
        raise NotImplementedError

    def create_in_hdx(self):
        # type: () -> None
        """Abstract method to check if resource exists in HDX and if so, update it, otherwise create it

        Returns:
            None
        """
        raise NotImplementedError

    def delete_from_hdx(self):
        # type: () -> None
        """Abstract method to deletes a resource from HDX

        Returns:
            None
        """
        raise NotImplementedError

    @staticmethod
    def get_all_activities(configuration=None, **kwargs):
        # type: (Optional[Configuration], Any) -> List['Activity']
        """Get all users in HDX

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            **kwargs: See below
            id (str): Id or name of the package
            offset (int): Where to start getting activity items from. Defaults to 0.
            limit (int): Maximum number of activities to return. Defaults to 31.

        Returns:
            List[Activity]: List of all users in HDX
        """
        activity = Activity(configuration=configuration)
        result = activity._write_to_hdx('package list', kwargs)
        activities = list()
        if result:
            for activitydict in result:
                activity = Activity(activitydict, configuration=configuration)
                activities.append(activity)
        else:
            logger.debug(result)
        return activities
