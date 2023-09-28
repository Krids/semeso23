from abc import ABC, abstractmethod
from typing import Dict, List

class FeatureStrategy(ABC):

    @abstractmethod
    def execute(self, data: List[Dict]) -> List[Dict]:
        """This function is a generic way to create a
        new feature.

        Args:
            data (List[Dict]): List of transctions' dict.

        Returns:
            List[Dict]: List of transactions' dict with
            the new feature. 
        """        
        pass
