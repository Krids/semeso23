from abc import ABC, abstractmethod

class FeatureStrategy(ABC):

    @abstractmethod
    def execute(self):
        pass
