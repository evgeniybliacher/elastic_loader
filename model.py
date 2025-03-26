import os
from typing import Annotated

from pydantic import BaseModel, DirectoryPath, Field, StringConstraints, computed_field


class InjestTask(BaseModel):
    index:str = Annotated[str, StringConstraints(strip_whitespace=True)]
    pipeline:str=Field(None)
    chunk_size:int = Field(strict=True, gt=0, default=1000)
    host:str = Annotated[str, StringConstraints(strip_whitespace=True, pattern=r'(https?:\/\/)[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})/gi')]
    port:int = Field(frozen=True,ge=0)
    user:str = Annotated[str, StringConstraints(strip_whitespace=True)]
    password:str = Annotated[str, StringConstraints(strip_whitespace=True)]
    data_folder:DirectoryPath
    sleep_between_chunks:float = Field(strict=True, ge=0, default=0.3)
    
    @computed_field
    @property
    def total_count(self) -> int:
        return calculate_number_of_files(self.data_folder)


def calculate_number_of_files(data_folder:str, extension:str=".json")->int:
    return len([name for name in os.listdir(data_folder) if os.path.isfile(os.path.join(data_folder, name)) and os.path.join(data_folder, name).endswith(extension)])